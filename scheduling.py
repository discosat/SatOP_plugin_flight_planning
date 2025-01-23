import io
import os
from pydantic import BaseModel
from fastapi import APIRouter, Depends, Request, HTTPException, status, BackgroundTasks
import logging

import sqlalchemy

from satop_platform.components.syslog import models
from satop_platform.plugin_engine.plugin import Plugin
from satop_platform.components.groundstation.connector import GroundstationConnector, GroundstationRegistrationItem, FramedContent
from satop_platform.components.restapi import exceptions

import uuid
from uuid import UUID

logger = logging.getLogger('plugin.scheduling')

class FlightPlan(BaseModel):
    flight_plan: dict
    datetime: str
    gs_id: str
    sat_name: str
    
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "flight_plan": 
                    {
                        "name": "commands",
                        "body": [
                            {
                                "name": "repeat-n",
                                "count": 10,
                                "body": [
                                    {
                                        "name": "gpio-write",
                                        "pin": 16,
                                        "value": 1
                                    },
                                    {
                                        "name": "wait-sec",
                                        "duration": 1
                                    },
                                    {
                                        "name": "gpio-write",
                                        "pin": 16,
                                        "value": 0
                                    },
                                    {
                                        "name": "wait-sec",
                                        "duration": 1
                                    }
                                ]
                            }
                        ]
                    },
                    "datetime": "2025-01-01T12:12:30+01:00",
                    "gs_id": "86c8a92b-571a-46cb-b306-e9be71959279",
                    "sat_name": "DISCO-2"
                }
            ]
        }
    }

class Scheduling(Plugin):
    def __init__(self, *args, **kwargs):
        plugin_dir = os.path.dirname(os.path.realpath(__file__))
        super().__init__(plugin_dir, *args, **kwargs)

        if not self.check_required_capabilities(['http.add_routes']):
            raise RuntimeError

        self.api_router = APIRouter()
        self.flight_plans_missing_approval: dict[UUID, dict] = dict()

        @self.api_router.post(
                '/save', 
                summary="Takes a flight plan and saves it for approval.",
                description="Takes a flight plan and saves it locally for later approval.",
                response_description="A message indicating the result of the scheduling or a dictionary with the message and the flight plan ID.",
                status_code=201, 
                dependencies=[Depends(self.platform_auth.require_login)]
                )
        async def new_flihtplan_schedule(flight_plan:FlightPlan, req: Request) -> dict[str, str] | str:
            user_id = req.state.userid

            if flight_plan.sat_name is None or flight_plan.sat_name == "":
                logger.info(f"User '{user_id}' sent flightplan for approval but rejected due to: FLIGHTPLAN - MISSING REFERENCE TO SATELLITE")
                return "Rejected, Missing Satellite reference"
            
            if flight_plan.datetime is None or flight_plan.datetime == "":
                logger.info(f"User '{user_id}' sent flightplan for approval but rejected due to: FLIGHTPLAN - MISSING DATETIME")
                return "Rejected, Missing datetime"
            
            if flight_plan.gs_id is None or flight_plan.gs_id == "":
                logger.info(f"User '{user_id}' sent flightplan for approval but rejected due to: FLIGHTPLAN - MISSING REFERENCE TO GS ID")
                return "Rejected, Missing GS ID"

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

            # TODO: return artiifact flight plan id instead of local "flight_plans_missing_approval" flight plan id.
            return {
                "message": f"Flight plan scheduled for approval", 
                "fp_id": f"{flight_plan_uuid}"
            }


        @self.api_router.post(
                '/approve/{uuid}', 
                summary="Approve a flight plan for transmission to a ground station",
                description=
"""
Approve or reject a flight plan for transmission to a ground station.
The flight plan is identified by the UUID provided in the URL.

If the flight plan is rejected, it will not be sent to the ground station and will be removed from the local list of flight plans missing approval.

If the flight plan is approved, a message will first return to the sender acknowledging that the request was received, and then the approved flight plan will be compiled and sent to the ground station.
""",
                response_description="A message indicating the result of the approval",
                # responses={**exceptions.NotFound("Flight plan not found").response},
                status_code=202, 
                dependencies=[Depends(self.platform_auth.require_login)]
                )
        async def approve_flight_plan(flight_plan_uuid:str, approved:bool, request: Request, background_tasks: BackgroundTasks) -> dict[str, str]: # TODO: maybe require the GS id here instead.
            # """Approve a flight plan for transmission to a ground station

            # Args:
            #     flight_plan_uuid (str): Identifier of the flight plan to approve
            #     approved (bool): Whether the flight plan is approved or not
                
            # Raises:
            #     HTTPException: If the flight plan is not found

            # Returns:
            #     (str) or (list(str)): An exception message or a message indicating the result of the approval
            # """
            user_id = request.state.userid
            flight_plan_uuid = UUID(flight_plan_uuid)
            flight_plan_with_datetime:FlightPlan = self.flight_plans_missing_approval.get(flight_plan_uuid)
            if flight_plan_with_datetime is None:
                logger.debug(f"Flight plan with uuid '{flight_plan_uuid}' was requested by user '{user_id}' but was not found")
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail='Flight plan not found')
            
            # LOGGING: User approves flight plan - user action and flight plan artifact, compiled flight plan artifact, GS id
            flight_plan_gs_id = UUID(flight_plan_with_datetime.gs_id)
            
            if not approved:
                logger.debug(f"Flight plan with uuid '{flight_plan_uuid}' was not approved by user: {user_id}")
                return {"message": "Flight plan not approved by user"}
            logger.debug(f"Flight plan with uuid '{flight_plan_uuid}' was approved by user: {user_id}")

            
            logger.debug(f"found flight plan: {flight_plan_with_datetime}")

            # Compile the flight plan
            # TODO: compile in seperate thread
            compiled_plan, artifact_id = await self.call_function("Compiler","compile", flight_plan_with_datetime.flight_plan, user_id)
            
            background_tasks.add_task(self._do_send_to_gs, flight_plan_uuid, compiled_plan, artifact_id, user_id)

            return {"message": "Flight plan approved and scheduled for transmission to ground station."}

    async def _do_send_to_gs(self, flight_plan_uuid, compiled_plan, artifact_id, user_id):
        """Send the compiled plan to the GS client

        Args:
            flight_plan_uuid (UUID): Identifier of the flight plan to approve
            compiled_plan (dict): The compiled flight plan
            artifact_id (str): Identifier of the compiled flight plan
            user_id (str): Identifier of the user who performed this action
        """
        # Send the compiled plan to the GS client
        logger.debug(f"\nsending compiled plan to GS: \n{compiled_plan}\n")
        flight_plan_with_datetime:FlightPlan = self.flight_plans_missing_approval.pop(flight_plan_uuid)
        flight_plan_gs_id = UUID(flight_plan_with_datetime.gs_id)

        gs_rtn_msg = await self.send_to_gs(
                        artifact_id, 
                        compiled_plan, 
                        flight_plan_gs_id, 
                        flight_plan_with_datetime.datetime,
                        flight_plan_with_datetime.sat_name
                    )           
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
                    predicate=models.Predicate(descriptor='sentTo'),
                    object=models.Entity(type='system',id=str(flight_plan_gs_id))
                    )
                ]
            )
        )
    
    # TODO: If artifact_id is not used, remove it from the function signature
    async def send_to_gs(self, artifact_id:str, compiled_plan:dict, gs_id:UUID, datetime:str, satellite:str):
        """Send the compiled plan to the GS client

        Args:
            artifact_id (str): Identifier of the compiled flight plan
            compiled_plan (dict): The compiled flight plan
            gs_id (UUID): Identifier of the ground station
            datetime (str): The datetime of the transmission
            satellite (str): The satellite to which the transmission is scheduled

        Returns:
            (str): The response from the GS client
        """
        gs = self.gs_connector.registered_groundstations.get(gs_id)
        if gs is None:
            logger.error(f"GS with id '{gs_id}' not found")
            return "GS not found"
        
        # Send the compiled plan to the GS client
        frame = FramedContent(
            header_data={
                'type' : 'schedule_transmission',
                'data' : {
                    'time' : datetime,
                    'satellite': satellite
                }
            },
            frames = [
                compiled_plan
            ]
        )

        return await self.gs_connector.send_control(gs_id, frame)


    
    def startup(self):
        """Startup protocol for the plugin
        """
        super().startup()
        logger.info(f"Running '{self.name}' statup protocol")
    
    def shutdown(self):
        """Shutdown protocol for the plugin
        """
        super().shutdown()
        logger.info(f"'{self.name}' Shutting down gracefully")