import logging
import json
from rest_framework.decorators import api_view
from apicaproject.utils import ApiResponse
from lru.lru import LRU

logger = logging.getLogger(__name__)


@api_view(['GET', ])
def getCacheValue(request):
    response = ApiResponse(data_type={})
    obj = LRU()
    key = request.GET.get("key")
    if not key:
        response.else_block(message='Missing parameter key', status_code=401)
    else:
        value = obj.get(key)
        if value != None:
            response.data.update({"value":value})
        else:
            response.else_block(message='Value is not found', status_code=401)

    return response.response_json()

@api_view(['POST', ])
def setCacheValue(request):
    response = ApiResponse(data_type={})
    body = request.body.decode()
    if body:
        try:
            body = json.loads(body)
        except Exception:
            response.else_block(message='Invalid request body data-type')
        obj = LRU()
        response = obj.set(body)
    
    else:
        response.else_block(message='Request body not available')
    
    return response.response_json()