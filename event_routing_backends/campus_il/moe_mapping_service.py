import logging, json, re
from enum import Enum
from tokenize import String
from event_routing_backends.campus_il.configuration import config
from common.djangoapps.student.models import CourseAccessRole
from social_django.models import UserSocialAuth
from openedx.core.djangoapps.bookmarks.models import XBlockCache
from django.core.cache import cache

class FieldTypes(Enum):
    TEXT = 'Text'
    LANG = 'Language'
    DURATION = 'Duration'
    IDENTIFIER = 'Identifier'
    OBJ = 'Object'
    
class IdsType(Enum):
    COURSE = 'Course'
    BLOCK = 'Block'

class MOEMapping():
    #%% Props / Vars section
    
    # Create a mapping dictionary for verbs
    verb_mapping = {
        "http://adlnet.gov/expapi/verbs/registered": "https://lxp.education.gov.il/xapi/moe/verbs/join",
        "http://id.tincanapi.com/verb/unregistered": "https://lxp.education.gov.il/xapi/moe/verbs/leave",
        "http://adlnet.gov/expapi/verbs/attempted": "https://lxp.education.gov.il/xapi/moe/verbs/attempted",
        "http://adlnet.gov/expapi/verbs/answered": "https://lxp.education.gov.il/xapi/moe/verbs/answered",
        "https://w3id.org/xapi/acrossx/verbs/evaluated": "https://lxp.education.gov.il/xapi/moe/verbs/scored",
        "http://adlnet.gov/expapi/verbs/completed": "https://lxp.education.gov.il/xapi/moe/verbs/completed",
        "https://w3id.org/xapi/video/verbs/played": "https://lxp.education.gov.il/xapi/moe/verbs/played",
        "https://w3id.org/xapi/video/verbs/paused": "https://lxp.education.gov.il/xapi/moe/verbs/paused",
        "https://w3id.org/xapi/video/verbs/seeked": "https://lxp.education.gov.il/xapi/moe/verbs/watched"
    }

    # Create a mapping dictionary for activities
    activity_mapping = {
        "http://adlnet.gov/expapi/activities/question": "https://lxp.education.gov.il/xapi/moe/activities/question",
        "http://adlnet.gov/expapi/activities/cmi.interaction": "https://lxp.education.gov.il/xapi/moe/activities/question",
        "https://w3id.org/xapi/video/activity-type/video": "https://lxp.education.gov.il/xapi/moe/activities/video",
        "http://adlnet.gov/expapi/activities/course": "https://lxp.education.gov.il/xapi/moe/activities/course"
    }
    
    # Default text value
    default_text_value = 'No Text'
    
    #%% Init section    
    
    def __init__(self):
        pass

    #%% Public section
    
    def map_event(self, event=None, event_str = ''):
        event = json.loads(event_str) if event_str else event
        
        # prepare event data
        self.__prepare_event(event)
        
        _course_id = self.__get_course_block_id(event, IdsType.COURSE)
        _block_id = self.__get_course_block_id(event, IdsType.BLOCK)
        logging.info(f'MOE: Mapping CampusIL course_id: {_course_id}, block_id: {_block_id}')
        
        # Mapping logic to convert to external organization JSON format
        external_event = {
            "id": event.get("id", ""),
            "timestamp": event.get("timestamp", ""),
            "version": event.get("version", ""),
            
            
            "actor": {
                "objectType": event["actor"].get("object_type", event["actor"].get("objectType", {})),
                "account": self.__add_field_if_exist(event["actor"]["account"], {
                    "homePage": FieldTypes.IDENTIFIER,
                    "name": FieldTypes.TEXT,
                })
            },
            
            
            "verb": {
                "id": event["verb"]["id"],
                "display": event["verb"]["display"]
            },

        
            "object": {
                "objectType": event["object"].get("object_type", event["object"].get("objectType", {})),
                "id": event["object"]["id"],
                "definition": self.__add_field_if_exist(event["object"]["definition"], {
                    "type": FieldTypes.TEXT,
                    "name": FieldTypes.LANG,
                    "description": FieldTypes.LANG
                }, course_id=_course_id, block_id=_block_id)
            },
            #result
            #context
        }
        
        # add result
        if "result" in event:
            
            external_event["result"] = self.__add_field_if_exist(event["result"], {
                    "success": FieldTypes.TEXT,
                    "completion": FieldTypes.TEXT,
                    "duration": FieldTypes.DURATION,
                })
            if "score" in event["result"]:
                external_event["result"]["score"] = self.__add_field_if_exist(event["result"]["score"], {
                    "scaled": FieldTypes.TEXT,
                    "raw": FieldTypes.TEXT,
                    "min": FieldTypes.TEXT,
                    "max": FieldTypes.TEXT,
                })
            if "extensions" in event["result"]:
                external_event["result"]["extensions"] = event["result"]["extensions"]
        
        # add context
        if "context" in event:
            for parent in event.get("context", {}).get("contextActivities", {}).get("parent", []):
                _parent = self.__add_field_if_exist(parent, {
                        "id": FieldTypes.TEXT,
                        "objectType": FieldTypes.TEXT,
                    })
                if "definition" in parent:
                    _parent["definition"] = self.__add_field_if_exist(parent["definition"], {
                            "name": FieldTypes.LANG,
                            "type": FieldTypes.TEXT,
                        })
                external_event.setdefault("context", {}).setdefault("contextActivities", {}).setdefault("parent", []).append(_parent)  
        
        # add instructor information
        _instructorNode = self.__get_intrsuctor_node(_course_id)
        if _instructorNode:
            external_event.setdefault("context", {})["instructor"] = _instructorNode
        
        # map CampusIL events to MOE events
        external_event = self.__map_fields_data(external_event)
        
        return external_event
    
    def is_relevant_event(self, verb_id):
        return verb_id in self.verb_mapping

    #%% Private section
    
    # map event's verb and object to MOE event verb and object
    def __map_fields_data(self, event):

        # Perform the mapping
        mapped_data = event.copy()

        # Map the verb
        if "verb" in event:
            verb_id = event["verb"]["id"]
            if verb_id in self.verb_mapping:
                mapped_data["verb"]["id"] = self.verb_mapping[verb_id]

        # Map the activity
        _activity_type = event.get("object", {}).get("definition", {}).get("type", None)
        if _activity_type and _activity_type in self.activity_mapping:
            event["object"]["definition"]["type"] = self.activity_mapping[_activity_type]
        
        return mapped_data
    
    def __add_field_if_exist(self, section, fieldsTypes, course_id=None, block_id=None):
        _output = {}
        _extensions_section = 'extensions'
        
        for field_name, field_type in fieldsTypes.items():
            if field_name in section:
                #if field_type is FieldTypes.OBJ:
                #    _output[field_name] = self.__add_field_if_exist(section[field_name])
                if field_type is FieldTypes.LANG:
                    _output[field_name] = self.__get_object_definition_name(section[field_name])
                elif field_type is FieldTypes.IDENTIFIER:
                    _output[field_name] = self.__get_user_id_identifier(section["name"])
                else:
                    _output[field_name] = section[field_name]
            elif field_type is FieldTypes.DURATION:
                _seconds = int(section.get(_extensions_section, {}).get(config.Get("MAPPING_EXTENSIONS_TIME"), 0))
                _output[field_name] = self.__convert_seconds_to_hms(_seconds)
            elif field_type is FieldTypes.LANG and course_id and block_id:
                _value = self.__get_block_title(course_id, block_id).strip() or self.default_text_value
                _key = self.__detect_language(_value)
                _output[field_name] = { _key: _value }
        
        return _output
    
    def __get_object_definition_name(self, value):
        _output = {}
        
        if type(value) is dict:
            for key in value:
                _lang = key.split('-')[0]
                _value = str(value[key]).strip() or self.default_text_value
                _lang = self.__detect_language(_value)
                _output[_lang] = _value
        
        return _output 
    
    def __detect_language(self, text):
        _output = "en"
        _cleaned_text = re.sub(r'[0-9\-\'"().,;:!?\[\]{}]', '', text).strip()
        
        # Check if all the characters are in the ASCII range
        if all(ord(char) < 128 for char in _cleaned_text):
            _output = "en"
        # Check if it's possibly Hebrew by looking at the Unicode block
        elif all('\u0590' <= char <= '\u05FF' or char.isspace() for char in _cleaned_text):
            _output = "he"
        # Check if it's possibly Arabic by looking at the Unicode block
        elif all('\u0600' <= char <= '\u06FF' or char.isspace() for char in _cleaned_text):
            _output = "ar"
        elif all('\u0400' <= char <= '\u04FF' or char.isspace() for char in _cleaned_text):
            _output = "ru"
        
        return _output
    
    def __get_user_id_identifier(self, number):
        if re.match(r'^[0-9]+$', number):
            return config.Get("MAPPING_IDENTIFIER_MOE")
        elif re.match(r'^[0-9a-fA-F]+$', number):
            return config.Get("MAPPING_IDENTIFIER_CAMPUSIL")
        else:
            return config.Get("MAPPING_IDENTIFIER_UNKNOWN")
    
    def __convert_seconds_to_hms(self, seconds):
        seconds = 0 if seconds is None else seconds
        
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60

        hours_str = f'{hours}H' if hours > 0 else ''
        minutes_str = f'{minutes:02d}M' if minutes > 0 else ''
        seconds_str = f'{seconds:02d}.00S'
        
        duration_string = f"PT{hours_str}{minutes_str}{seconds_str}"
        return duration_string
    
    def __get_intrsuctor_node(self, course_id):
        cache_key = f'{config.Get("MAPPING_CACHE_INSTRUCTOR_PREFIX")}_{course_id}'
        anonymous_id = cache.get(cache_key)
        
        if not anonymous_id:
            teacher_course_role = CourseAccessRole.objects.filter(
                course_id=course_id,
                role='staff',
            ).exclude(
                user__email__endswith='campus.gov.il'
            ).first()
        
            # get teacher's IDM
            if teacher_course_role:
                social_auth = UserSocialAuth.objects.filter(user__id=teacher_course_role.user.id, provider='tpa-saml', uid__startswith='moe-edu-idm:').first()
                
                #logging.info(f"MOE: Teacher of CCX social_auth: {social_auth}")
                if social_auth:
                    anonymous_id = social_auth.uid.split(':')[1]
                    cache.add(cache_key, anonymous_id, int(config.Get("MAPPING_CACHE_EXPIRATION"))) #in seconds
        
        if anonymous_id:
            #logging.info(f"MOE: Teacher of CCX anonymous_id: {anonymous_id}")
            return {
                "objectType": "Agent",
                "account": {
                    "homePage": config.Get("MAPPING_IDENTIFIER_MOE"),
                    "name": anonymous_id
                }
            }
        
        return None
    
    # prepare course id or block id of the event
    def __get_course_block_id(self, event, id_type:IdsType):
        _output = ''
        _url = ''
        
        if id_type is IdsType.COURSE:
            _type = event.get("object", {}).get("definition", {}).get("type", None)
            if _type and _type == "http://adlnet.gov/expapi/activities/course":
                _url = event.get("object").get("id", None)
            else:
                _parents_arr = event.get("context", {}).get("contextActivities", {}).get("parent", None)
                if _parents_arr and len(_parents_arr) > 0:
                    _url = _parents_arr[0]["id"]
            
            _output = _url.split("/")[-1]
        elif id_type is IdsType.BLOCK:    
            _object = event.get("object", None)
            if _object and "block-v1" in _object["id"]:
                _url = _object["id"]
            
            _output = _url.split("/")[-1]
            _output = re.sub(r'\+ccx@\d+', '', _output)
            _output = _output[4:] if _output.startswith('ccx-') else _output
            
        return _output
    
    # get xblock title
    def __get_block_title(self, course_key, usage_key) -> str:
        cache_key = f'{config.Get("MAPPING_CACHE_BLOCK_PREFIX")}_{usage_key}'
        block_title = cache.get(cache_key)
        
        if not block_title:
            block_cache = XBlockCache.objects.filter(usage_key=usage_key).first()
            block_title = block_cache.display_name if block_cache else None
            cache.add(cache_key, block_title, int(config.Get("MAPPING_CACHE_EXPIRATION"))) #in seconds
            
        return block_title
    
    # prepare event
    def __prepare_event(self, event):
        
        # prepate the /course/ url of the input event
        for parent in event.get("context", {}).get("contextActivities", {}).get("parent", []):
            parent["id"] = parent.get("id").replace("/course/", "/courses/")