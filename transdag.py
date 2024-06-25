from sqlalchemy import create_engine
import sqlalchemy as sa
from dwh.__init__ import all
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from sqlalchemy import text

@task(task_id="get_info_fromdb")
def fetch_dwh_db():
    schemas = [{
        "STG": "User",
        "ODS": "User",
        "VALIDATION": "UserAuth",
    } , {
        "STG":"Group",
        "ODS":"Group",
        "VALIDATION":"GroupAuth",
    },{
        "STG": "UserGroup",
        "ODS": "UserGroup",
        "VALIDATION": "UserGroupAuth",
    } ,{
        "STG": "AuthMethod",
        "ODS": "AuthMethod",
        "VALIDATION": "AuthMethodAuth",
    } ,{
        "STG": "UserSession",
        "ODS": "UserSession",
        "VALIDATION": "UserSessionAuth",
    } ,{
        "STG": "Scope",
        "ODS": "Scope",
        "VALIDATION": "ScopeAuth",
    } ,{
        "STG": "GroupScope",
        "ODS": "GroupScope",
        "VALIDATION": "GroupScopeAuth",
    } ,{
        "STG": "UserSessionScope",
        "ODS": "UserSessionScope",
        "VALIDATION": "UserSessionScopeAuth",
    } ,  {
        "STG":"UserMessageDelay",
        "ODS":"UserMessageDelay",
        "VALIDATION":"UserMessageDelayAuth"
    }, {
        "STG":"User",
        "ODS":"User",
        "VALIDATION":"UserM",
    }, {
        "STG":"ActionsInfo",
        "ODS":"ActionsInfo",
        "VALIDATION":"ActionsInfoM",
    }, {
        "STG":"Contacts",
        "ODS":"Contacts",
        "VALIDATION":"ContactsPh",
    }, {
        "STG":"Receiver",
        "ODS":"Receiver",
        "VALIDATION":"ReceiverPi",
    }, {
        "STG":"Alert",
        "ODS":"Alert",
        "VALIDATION":"AlertPi",
    }, {
        "STG":"Fetcher",
        "ODS":"Fetcher",
        "VALIDATION":"FetcherPi",
    }, {
        "STG":"Metric",
        "ODS":"Metric",
        "VALIDATION":"MetricPi",
    }, {
        "STG":"UnionMember",
        "ODS":"UnionMember",
        "VALIDATION":"UnionMemberPr",
    }, {
        "STG":"File",
        "ODS":"File",
        "VALIDATION":"FilePr",
    }, {
        "STG":"Category",
        "ODS":"Category",
        "VALIDATION":"CategorySv",
    }, {
        "STG":"Button",
        "ODS":"Button",
        "VALIDATION":"ButtonSv",
    }, {
        "STG":"Scope",
        "ODS":"Scope",
        "VALIDATION":"ScopeSv",
    }, {
        "STG":"WebhookStorage",
        "ODS":"WebhookStorage",
        "VALIDATION":"WebhookStorageSoc",
    }, {
        "STG":"VkGroups",
        "ODS":"VkGroups",
        "VALIDATION":"VkGroupsSoc",
    }, {
        "STG":"Creditials",
        "ODS":"Creditials",
        "VALIDATION":"CreditialsT",
    }, {
        "STG":"Room",
        "ODS":"Room",
        "VALIDATION":"RoomT",
    }, {
        "STG":"Lecturer",
        "ODS":"Lecturer",
        "VALIDATION":"LecturerT",
    }, {
        "STG":"Group",
        "ODS":"Group",
        "VALIDATION":"GroupT",
    }, {
        "STG":"Event",
        "ODS":"Event",
        "VALIDATION":"EventT",
    }, {
        "STG":"EventsLecturers",
        "ODS":"EventsLecturers",
        "VALIDATION":"EventsLecturersT",
    }, {
        "STG":"EventsRooms",
        "ODS":"EventsRooms",
        "VALIDATION":"EventsRoomsT",
    }, {
        "STG":"EventsGroups",
        "ODS":"EventsGroups",
        "VALIDATION":"EventsGroupsT",
    }, {
        "STG":"Photo",
        "ODS":"Photo",
        "VALIDATION":"PhotoT",
    }, {
        "STG":"CommentLecturer",
        "ODS":"CommentLecturer",
        "VALIDATION":"CommentLecturerT",
    }, {
        "STG":"CommentEvent",
        "ODS":"CommentEvent",
        "VALIDATION":"CommentEventT",
    }, {
        "STG":"UnionMember",
        "ODS":"UnionMember",
        "VALIDATION":"UnionMemberUnimember",
    }, {
        "STG":"Category",
        "ODS":"Category",
        "VALIDATION":"CategoryUd",
    }, {
        "STG":"Param",
        "ODS":"Param",
        "VALIDATION":"ParamUd",
    }, {
        "STG":"Source",
        "ODS":"Source",
        "VALIDATION":"SourceUd",
    }, {
        "STG":"Info",
        "ODS":"Info",
        "VALIDATION":"InfoUd",
    }]
        #для всех классов валидации таким образом прописывать

    dwhuri = (
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
    )
    dwh_sql_engine = create_engine(dwhuri) #создаем движок
    with dwh_sql_engine.connect() as conn:
        for i in range(len(schemas)):
            res = conn.execute(sa.text(f'''SELECT * FROM {schemas[i]["STG"]}''')).fetchall()
            for obj in res:
                valid = schemas[i]["VALIDATION"](**obj)
                conn.execute(text(f'''INSERT INTO {schemas[i]["ODS"]} VALUES {valid}'''))
                conn.commit()
