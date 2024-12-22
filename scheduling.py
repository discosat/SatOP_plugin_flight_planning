import io
import os
from fastapi import APIRouter, Depends, Request
import logging

import sqlalchemy

from satop_platform.components.syslog import models
from satop_platform.plugin_engine.plugin import Plugin
from satop_platform.components.groundstation.connector import GroundstationConnector, GroundstationRegistrationItem

import uuid
from uuid import UUID

logger = logging.getLogger('plugin.scheduling')


class Scheduling(Plugin):
    def __init__(self, *args, **kwargs):
        plugin_dir = os.path.dirname(os.path.realpath(__file__))
        super().__init__(plugin_dir, *args, **kwargs)

        if not self.check_required_capabilities(['http.add_routes']):
            raise RuntimeError

        self.api_router = APIRouter()
        self.flight_plans_missing_approval: dict[UUID, dict] = dict()

        @self.api_router.post('/compile', status_code=201, dependencies=[Depends(self.platform_auth.require_login)])
        async def new_flihtplan_schedule(flight_plan:dict, req: Request):

            # LOGGING: User saves flight plan - user action and flight plan artifact

            flight_plan_as_bytes = io.BytesIO(str(flight_plan).encode('utf-8'))
            try:
                artifact_in_id = self.sys_log.create_artifact(flight_plan_as_bytes, filename='detailed_flight_plan.json').sha1
                logger.info(f"Received new detailed flight plan with artifact ID: {artifact_in_id}, scheduled for approval")
            except sqlalchemy.exc.IntegrityError as e: 
                # Artifact already exists
                artifact_in_id = e.params[0]
                logger.info(f"Received existing detailed flight plan with artifact ID: {artifact_in_id}")

            # -- actual scheduling --
            
            flight_plan_uuid = uuid.uuid4()
            logger.warning(f"Flight plan scheduled for approval, id: {flight_plan_uuid}")
            self.flight_plans_missing_approval[flight_plan_uuid] = flight_plan
            
            # -- end of scheduling --

            self.sys_log.log_event(models.Event(
                descriptor='FlightplanSaveEvent',
                relationships=[
                    models.EventObjectRelationship(
                        predicate=models.Predicate(descriptor='startedBy'),
                        object=models.Entity(type=models.EntityType.user, id=req.state.userid)
                        ),
                    models.EventObjectRelationship(
                        predicate=models.Predicate(descriptor='created'),
                        object=models.Artifact(sha1=artifact_in_id)
                        )
                    ]
                )
            )

            logger.info(f"Flight plan scheduled for approval; flight plan id: {flight_plan_uuid}")

            return {
                "message": f"Flight plan scheduled for approval", 
                "fp_id": f"{flight_plan_uuid}"
            }


        @self.api_router.post('/approve/{uuid}', status_code=201, dependencies=[Depends(self.platform_auth.require_login)])
        async def approve_flight_plan(flight_plan_uuid:str, approved:bool, request: Request): # TODO: maybe require the GS id here instead.
            
            # LOGGING: User approves flight plan - user action and flight plan artifact, compiled flight plan artifact, GS id
            user_id = request.state.userid
            flight_plan_uuid = UUID(flight_plan_uuid)
            flight_plan_gs_id = UUID(flight_plan_with_datetime["gs_id"])
            
            if not approved:
                logger.debug(f"Flight plan with uuid '{flight_plan_uuid}' was not approved by user: {user_id}")
                return {"message": "Flight plan not approved by user"}
            logger.debug(f"Flight plan with uuid '{flight_plan_uuid}' was approved by user: {user_id}")

            flight_plan_with_datetime = self.flight_plans_missing_approval.get(flight_plan_uuid)
            if flight_plan_with_datetime is None:
                logger.debug(f"Flight plan with uuid '{flight_plan_uuid}' was requested by user '{user_id}' but was not found")
                return {"message": "Flight plan not found"}
            
            logger.debug(f"found flight plan: {flight_plan_with_datetime}")

            # Compile the flight plan
            # TODO: compile in seperate thread
            compiled_plan, artifact_id = await self.call_function("Compiler","compile", flight_plan_with_datetime["flight_plan"], user_id)
            

            # Send the compiled plan to the GS client
            logger.debug(f"\nsending compiled plan to GS: \n{compiled_plan}\n")
            self.flight_plans_missing_approval.pop(flight_plan_uuid)

            gs_rtn_msg = await self.send_to_gs(artifact_id, compiled_plan, flight_plan_gs_id, flight_plan_with_datetime["datetime"])
            logger.debug(f"GS response: {gs_rtn_msg}")


            self.sys_log.log_event(models.Event(
                descriptor='ApprovedForSendOffEvent',
                relationships=[
                    models.EventObjectRelationship(
                        predicate=models.Predicate(descriptor='sentBy'),
                        object=models.Entity(type=models.EntityType.user, id=user_id)
                        ),
                    models.EventObjectRelationship(
                        predicate=models.Predicate(descriptor='used'),
                        object=models.Artifact(sha1=artifact_id)
                        ),
                    models.EventObjectRelationship(
                        predicate=models.Predicate(descriptor='setTo'),
                        object=models.Artifact(sha1=flight_plan_gs_id)
                        )
                    ]
                )
            )

            message = {
                "Flight plan approved and sent to GS",
                f"Flight plan: {compiled_plan}"
            }

            return {"message": message}


    # async def get_compiled_plan(self, artifact_id:str):
    #     # get the compiled plan from the database using fast api call
    #     """
    #     using call http://127.0.0.1:7889/api/log/artifacts/:hash were hash is the artifact_id
    #     """ 
    #     # async def get_compiled_plan(self, artifact_id: str):
    #     url = f"http://127.0.0.1:7889/api/log/artifacts/{artifact_id}"
    #     async with httpx.AsyncClient() as client:
    #         response = await client.get(url)
    #         response.raise_for_status()
    #         return response.json()
            
    async def send_to_gs(self, artifact_id:str, compiled_plan:dict, gs_id:UUID, datetime:str):
        """
        Send the compiled plan to the GS client
        """
        gs = self.gs_connector.registered_groundstations.get(gs_id)
        if gs is None:
            logger.error(f"GS with id '{gs_id}' not found")
            return "GS not found"
        
        # Send the compiled plan to the GS client
        return await self.gs_connector.send_control(gs_id, compiled_plan)


    
    def startup(self):
        super().startup()
        logger.info(f"Running '{self.name}' statup protocol")
    
    def shutdown(self):
        super().shutdown()
        logger.info(f"'{self.name}' Shutting down gracefully")