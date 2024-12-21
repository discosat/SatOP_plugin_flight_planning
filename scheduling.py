import io
import os
from fastapi import APIRouter, Depends, Request
import logging

import sqlalchemy

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
        async def new_flihtplan_schedule(flight_plan:dict):

            # flight_plan_as_bytes = io.BytesIO(str(flight_plan).encode('utf-8'))
            # try:
            #     artifact_in_id = self.sys_log.create_artifact(flight_plan_as_bytes, filename='flight_plan.json').sha1
            #     logger.info(f"Received new flight plan with artifact ID: {artifact_in_id}")
            # except sqlalchemy.exc.IntegrityError as e: 
            #     # Artifact already exists
            #     artifact_in_id = e.params[0]
            #     logger.info(f"Received existing flight plan with artifact ID: {artifact_in_id}")

            # self.flight_plans_missing_approval.append({"flight_plan": flight_plan, "uuid": uuid.uuid4()})
            this_uuid = uuid.uuid4()
            logger.warning(f"Flight plan scheduled for approval, id: {this_uuid}")
            self.flight_plans_missing_approval[this_uuid] = flight_plan

            return {"message": f"Flight plan scheduled for approval", "fp_id": f"{this_uuid}"}


            # Receives flight plan and datetime via this POST
            # compiled_plan, artifact_id = await self.call_function("Compiler","compile", flight_plan["flight_plan"], request)
            # self.flight_plans_missing_approval.append({"artifact_id": artifact_id, "datetime": flight_plan["datetime"]})


            # logger.debug(f"sending compiled plan to GS: \n{compiled_plan}")
            # logger.debug(f"flight plan scheduled for {flight_plan['datetime']}")

            # return {"message": f"Flight plan scheduled for approval, id: {this_uuid}"}


        @self.api_router.post('/approve/{uuid}', status_code=201, dependencies=[Depends(self.platform_auth.require_login)])
        async def approve_flight_plan(fp_uuid:str, approved:bool, request: Request): # TODO: maybe require the GS id here instead.

            # logger.debug(f"List of flight plans missing approval: {self.flight_plans_missing_approval}")
            
            if not approved:
                logger.debug(f"Flight plan with uuid '{fp_uuid}' was not approved by user: {request.state.userid}")
                return {"message": "Flight plan not approved by user"}
            logger.debug(f"Flight plan with uuid '{fp_uuid}' was approved by user: {request.state.userid}")

            flight_plan_with_datetime = self.flight_plans_missing_approval.get(UUID(fp_uuid))
            if flight_plan_with_datetime is None:
                logger.debug(f"Flight plan with uuid '{fp_uuid}' not found")
                return {"message": "Flight plan not found"}
            
            logger.debug(f"found flight plan: {flight_plan_with_datetime}")

            # Compile the flight plan
            # TODO: compile in seperate thread
            compiled_plan, artifact_id = await self.call_function("Compiler","compile", flight_plan_with_datetime["flight_plan"], fp_uuid)
            

            # Send the compiled plan to the GS client
            # TODO: send flight plan with datetime to GS
            logger.debug(f"\nsending compiled plan to GS: \n{compiled_plan}\n")
            self.flight_plans_missing_approval.pop(UUID(fp_uuid))

            message = {
                "Flight plan approved and sent to GS",
                f"Flight plan: {compiled_plan}"
            }

            gs_rtn_msg = await self.send_to_gs(artifact_id, compiled_plan, flight_plan_with_datetime["gs_id"], flight_plan_with_datetime["datetime"])
            logger.debug(f"GS response: {gs_rtn_msg}")

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
            
    async def send_to_gs(self, artifact_id:str, compiled_plan:dict, gs_id:str, datetime:str):
        """
        Send the compiled plan to the GS client
        """
        gs = self.gs_connector.registered_groundstations.get(gs_id)
        if gs_id is None:
            logger.error(f"GS with id '{gs_id}' not found")
            return "GS not found"
        
        # Send the compiled plan to the GS client
        return await gs.send_control(gs_id, compiled_plan)


    
    def startup(self):
        super().startup()
        logger.info(f"Running '{self.name}' statup protocol")
    
    def shutdown(self):
        super().shutdown()
        logger.info(f"'{self.name}' Shutting down gracefully")