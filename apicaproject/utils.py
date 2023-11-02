import logging
from rest_framework.response import Response

logger = logging.getLogger(__name__)

class ApiResponse:
    def __init__(self, data_type=None):
        self.status_code = 200
        self.data = data_type
        self.message = "Successful response"
        self.is_success = True
 
    def response_json(self, headers=None, content_type=None, response_data=None):
        if headers and isinstance(headers, dict):
            headers.update({"Access-Control-Allow-Origin": "*"})
        else:
            headers = {"Access-Control-Allow-Origin": "*"}
 
        if response_data:
            data = {
                "message": self.message,
            }
            data.update(response_data)
            data.update({
                "status_code": self.status_code,
                "is_success": True if (200 >= int(self.status_code) <= 300) else False
            })
            return Response(
                data=data,
                status=self.status_code, headers=headers, content_type=content_type            
            )
 
        return Response(data={
            "message": self.message,
            "data": self.data,
            "status_code": self.status_code,
            "is_success": True if (200 >= int(self.status_code) <= 300) else False
        }, status=self.status_code, headers=headers, content_type=content_type)
 
    def __str__(self):
        return f'<Response(Code={self.status_code}, Data={self.data}, Message={self.message})'
 
    def handel_exception(self, _logger=None, func_name=None, exc_msg=None, error_message=None, db_err=True):
        if _logger:
            _logger.error(f"{func_name} : {exc_msg}")
        else:
            logger.error(f"{func_name} : {exc_msg}")
        self.is_success = False
        self.status_code = 400
        if error_message:
            self.message = error_message
        else:
            self.message = "Database error" if db_err else "Something went wrong"
        if not self.data:
            self.data = None
        return self
 
    def else_block(self, message=None, data=None, status_code=None):
        self.is_success = True if status_code else False
        self.status_code = status_code if status_code else 400
        self.message = message if message else "Something went wrong"
        self.data = data
        return self
