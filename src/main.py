import httpx
from lxml import html
from typing import List
import json


class TableColumn:
    def __init__(self, name, column_type, description):
        self.name = name
        self.type = column_type
        self.description = description


class TableSchema:
    def __init__(self, name: str, schema: List[TableColumn] = []):
        self.name = name
        self.schema = schema


# download the html for the source page, contains information on available tables and api version
def get_source():
    r = httpx.get('https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview')
    return html.fromstring(r.content)


# parse our the list of compatible default tables from the html (source)
def get_compatible_tables(source):
    table_elements = source.xpath('//*[@id="main"]/div[3]/ul[1]/li[*]')
    tables = list()
    for table in table_elements:
        if table.text is not None:
            tables.append(table.text)
        else:
            link_element = table.xpath('a')
            tables.append(link_element[0].text)
    return tables


# parse our api version from the source of the overview page
def get_api_version(source):
    template = source.xpath('//*[@id="main"]/div[3]/pre[1]/code')[0].text
    api_version = template.rstrip().split('api-version=')[1]
    return api_version


# pull column information for each table
def get_table_schema(table):
    r = httpx.get(f"https://learn.microsoft.com/en-us/azure/azure-monitor/reference/tables/{table}")
    source = html.fromstring(r.content)

    # some tables do not have published schema information. for those, we skip
    if r.status_code == 404:
        return None
    schema_elements = source.xpath("//table[2]/tbody/tr[*]")

    table = TableSchema(name=table)
    for schema_element in schema_elements:
        values = schema_element.xpath('td')
        column = TableColumn(name=values[0].text, column_type=values[1].text, description=values[2].text)
        table.schema.append(column)

    return table


def get_table_schemas(tables):
    table_schemas = list()
    for table in tables:
        schema = get_table_schema(table)

        # none is returned when table schema information is not found on microsoft's documentation
        if schema is None:
            continue
        table_schemas.append(schema)

    return table_schemas


# create the stream declaration part of the DCR
def create_stream_declarations(schemas: List[TableSchema]):
    streams = dict()
    for table_schema in schemas:
        streams[f"Custom-{table_schema.name}"] = {
            "columns": [{
                "name": column.name,
                "type": column.type
            } for column in table_schema.schema]
        }

    return streams


# create data flow part of the DCR
def create_data_flows(tables):
    data_flows = list()
    for table in tables:
        data_flows.append({
            "streams": [f"Custom-{table}"],
            "destinations": ["logAnalyticsWorkspace"],
            "transformKql": "source",
            "outputStream": f"Microsoft-{table}"
        })

    return data_flows


if __name__ == '__main__':
    source = get_source()

    # get all tables
    # tables = get_compatible_tables(source)

    # table list is hardcoded to most relevant tables. These are case-sensitive
    # no more than 10 streams/data flows can be implemented in a single DCR
    tables = ['AWSCloudTrail', 'AWSCloudWatch', 'AWSGuardDuty', 'AWSVPCFlow', 'CommonSecurityLog', 'GCPAuditLog',
              'GoogleCloudSCC', 'SecurityEvent', 'Syslog', 'WindowsEvent']

    # handle parsing of the microsoft webpage
    api_version = get_api_version(source)
    schemas = get_table_schemas(tables)

    # reformat data for DCR
    stream_declarations = create_stream_declarations(schemas)
    data_flows = create_data_flows(tables=tables)

    # read in template
    template_json = json.load(open('TEMPLATE_DCR.json', 'r'))

    # replace values in template with new values
    template_json['resources'][0]['apiVersion'] = api_version
    template_json['resources'][0]['properties']['streamDeclarations'] = stream_declarations
    template_json['resources'][0]['properties']['dataFlows'] = data_flows

    # truncate file (in case it exists), and write new DCR
    with open('ARM_DCR_CRIBL.json', 'w+') as o_file:
        o_file.truncate()
        json.dump(template_json, o_file, indent=2)
