import base64
import json
from collections.abc import Generator
from typing import Iterator

from RequestHandler import RequestHandler
from ResourceEnum import ResourceEnum
from ResponseObject import ResponseObject
from ZoekParameterOTL import ZoekParameterOTL
from ZoekParameterPayload import ZoekParameterPayload


class EMInfraImporter:
    def __init__(self, request_handler: RequestHandler):
        self.request_handler = request_handler
        self.request_handler.requester.first_part_url += 'eminfra/'
        self.paging_cursors = {}

    def get_events_from_proxyfeed(self, resource: str, page_num: int = -1, page_size: int = 100) -> dict:
        if page_num == -1:
            url = f"feedproxy/feed/{resource}"
        else:
            url = f"feedproxy/feed/{resource}/{page_num}/{page_size}"
        return self.request_handler.get_jsondict(url=url)

    def get_objects_from_oslo_search_endpoint(self, resource: ResourceEnum, cursor: str | None = None, size: int = 100
                                              ) -> ResponseObject:
        url = f'core/api/otl/{resource.value}/search'
        otl_zoekparameter = ZoekParameterOTL(size=size, from_cursor=cursor)

        if resource == ResourceEnum.agents:
            otl_zoekparameter.expansion_field_list = ['contactInfo']

        json_data = otl_zoekparameter.to_dict()

        response = self.request_handler.perform_post_request(url=url, json_data=json_data)

        decoded_string = response.content.decode("utf-8")

        return ResponseObject(graph=json.loads(decoded_string), headers=dict(response.headers))
