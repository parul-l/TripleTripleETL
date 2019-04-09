import os
from xml.dom import minidom

from triple_triple_etl.load.postgres.postgres_connection import get_cursor


def create_event_code_table():
    query = """
        BEGIN TRANSACTION;

        CREATE TABLE event_code (
              event_id INTEGER
            , event_description VARCHAR(100)
            , detail_id INTEGER
            , detail_description VARCHAR(100)
            , PRIMARY KEY (event_id, detail_id)
        );

        END TRANSACTION;
    """
    with get_cursor() as cursor:
        cursor.execute(query)


def event_code_query(
    event_id,
    event_description,
    detail_id,
    detail_description
):
        query = """
        INSERT INTO event_code (
            event_id
            , event_description
            , detail_id
            , detail_description
        )
            VALUES (
            {event_id}
            , '{event_description}'
            , {detail_id}
            , '{detail_description}'
            );
        """.format(
                event_id=event_id,
                event_description=event_description,
                detail_id=detail_id,
                detail_description=detail_description
            )
        with get_cursor() as cursor:
            cursor.execute(query)    
    

def insert_event_codes(event_code_data):
    for event in event_code_data.getElementsByTagName('event'):
        event_id = int(event.attributes['id'].value)
        event_description = event.attributes['text'].value
        details = event.getElementsByTagName('detail')
        if details:
            for detail in details:
                detail_id = int(detail.attributes['id'].value)
                detail_description = detail.attributes['text'].value
                event_code_query(
                    event_id=event_id,
                    event_description=event_description,
                    detail_id=detail_id,
                    detail_description=detail_description
                )

        else:
            detail_id = -1
            detail_description = 'None'
            event_code_query(
                event_id=event_id,
                event_description=event_description,
                detail_id=detail_id,
                detail_description=detail_description
            )


if __name__ == '__main__':
    
    filepath = os.path.abspath('triple_triple_etl/nba_event_codes.xml')
    event_code_data = minidom.parse(filepath)

    insert_event_codes(event_code_data)

