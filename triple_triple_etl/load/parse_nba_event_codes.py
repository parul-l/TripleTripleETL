import os
import pandas as pd
import xml
import xml.etree.ElementTree as ET

from triple_triple_etl.constants import DATA_DIR


def get_event_code(
        etree_root: xml.etree.ElementTree.Element,
        branch: str = '.sports-event-codes/event-codes/event'
):
    """
        Gets dictionary with key = event code, and value = generic action
    """
    return {
        x.attrib['id']: x.attrib['text']
        for x in etree_root.findall(branch)
    }
    

def parse_event_code(event: xml.etree.ElementTree.Element):

    event_code = event.attrib['id']
    event_action = event.attrib['text']

    # if there are any elements in the details
    if list(event.iter('detail')):
        event_group = [
            [ 
                int(event_code),
                event_action.lower(),
                int(subevent.attrib['id']),
                subevent.attrib['text'].lower()
                
            ]
            for subevent in event.iter('detail')
        ]
    else:
        event_group = [[int(event_code), event_action.lower(), None, None]]

    columns = ['event_code', 'event_action', 
               'event_subcode', 'event_subaction']

    return pd.DataFrame(event_group, columns=columns)


def event_codes_to_pandas(
    etree_root: xml.etree.ElementTree.Element,
    branch: str = '.sports-event-codes/event-codes/event'
):
    # initiate empty dataframe
    columns = ['event_code', 'event_action',
               'event_subcode', 'event_subaction']
    df = pd.DataFrame(data=[], columns = columns)

    events = etree_root.findall(branch)
    for event in events:
        df = pd.concat([df,parse_event_code(event)], axis=0)
    
    return df


if __name__ == '__main__':
    filepath = os.path.join(DATA_DIR, 'nba-event-codes.XML')
    with open(filepath) as f:
        data = f.read()
    
    # convert to xml
    etree_root = ET.fromstring(data)
    df_codes = event_codes_to_pandas(etree_root=etree_root)
