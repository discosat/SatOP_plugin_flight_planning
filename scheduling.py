import dataclasses
import os
from fastapi import APIRouter, Depends, Request
import logging

from satop_platform.plugin_engine.plugin import Plugin
# from satop_platform.components.groundstation.connector import GroundStationConnector

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

            # self.flight_plans_missing_approval.append({"flight_plan": flight_plan, "uuid": uuid.uuid4()})
            this_uuid = uuid.uuid4()
            logger.warning(f"Flight plan scheduled for approval, id: {this_uuid}")
            self.flight_plans_missing_approval[this_uuid] = flight_plan

            return {"message": f"Flight plan scheduled for approval, id: {this_uuid}"}


            # Receives flight plan and date and time via this POST
            # compiled_plan, artifact_id = await self.call_function("Compiler","compile", flight_plan["flight_plan"], request)
            # self.flight_plans_missing_approval.append({"artifact_id": artifact_id, "date": flight_plan["date"], "time": flight_plan["time"]})


            # logger.debug(f"sending compiled plan to GS: \n{compiled_plan}")
            # logger.debug(f"flight plan scheduled for {flight_plan['date']} at {flight_plan['time']}")

            # return {"message": f"Flight plan scheduled for approval, id: {this_uuid}"}


        @self.api_router.post('/approve/{uuid}', status_code=201, dependencies=[Depends(self.platform_auth.require_login)])
        async def approve_flight_plan(fp_uuid:str, approved:bool, request: Request):

            logger.debug(f"List of flight plans missing approval: {self.flight_plans_missing_approval}")
            
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
            
    # def test_function(request: Request, path_parameter: path_param):

    #     request_example = {"test" : "in"}
    #     host = request.client.host
    #     data_source_id = path_parameter.id

    #     get_test_url= f"http://{host}/test/{id}/"
    #     get_inp_url = f"http://{host}/test/{id}/inp"

    #     test_get_response = requests.get(get_test_url)
    #     inp_post_response = requests.post(get_inp_url , json=request_example)
    #     if inp_post_response .status_code == 200:
    #         print(json.loads(test_get_response.content.decode('utf-8')))

        
    
    def startup(self):
        super().startup()
        logger.info(f"Running '{self.name}' statup protocol")
    
    def shutdown(self):
        super().shutdown()
        logger.info(f"'{self.name}' Shutting down gracefully")