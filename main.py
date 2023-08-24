from cassandra.cluster import Cluster
import xmltodict
import json

# cluster = Cluster()
# session = cluster.connect('samplemeta')



def insert_Data():
    metadata = {
        "xmlFileInfo": {
            "fileName": "employees.xml",
            "filePath": "./Data_sources/dataset.xml",
            "fileSize": "1024 KB",
            "createdDate": "2023-08-01",
            "lastModifiedDate": "2023-08-15"
        },
        "author": {
            "name": "John Doe",
            "email": "john@example.com"
        },
        "organization": {
            "name": "ABC Tech",
            "department": "HR",
            "location": "New York"
        },
        "dataDescription": {
            "description": "XML data containing employee information",
            "format": "XML",
            "version": "1.0"
        },
        "dataFields": [
            {
                "name": "id",
                "type": "integer",
                "description": "Unique identifier for each employee"
            },
            {
                "name": "name",
                "type": "string",
                "description": "Name of the employee"
            },
            {
                "name": "department",
                "type": "string",
                "description": "Department in which the employee works"
            },
            {
                "name": "position",
                "type": "string",
                "description": "Job position of the employee"
            },
            {
                "name": "salary",
                "type": "decimal",
                "description": "Salary of the employee"
            },
            {
                "name": "hireDate",
                "type": "date",
                "description": "Date when the employee was hired"
            }
        ]
    }

    file_name = metadata["xmlFileInfo"]["fileName"]
    print(file_name, "\n")
    xml_file_info = metadata["xmlFileInfo"]

    first = "{"
    for index, (key, value) in enumerate(xml_file_info.items()):
        first += f"{key}: '{value}'"
        if index == len(xml_file_info.items()) - 1:
            continue
        first += ","

    first  += "}"
    author_info = metadata["author"]
    second = "{"
    for index,(key, value) in enumerate(author_info.items()):
        second += f"{key}: '{value}'"
        if index == len(author_info.items()) - 1:
            continue
        second += ","
    second += "}"
    organization_info = metadata["organization"]
    third = "{"
    for index,(key, value) in enumerate(organization_info.items()):
        third += f"{key}: '{value}'"
        if index == len(organization_info.items()) - 1:
            continue
        third += ","
    third += "}"
    data_description = metadata["dataDescription"]
    fourth = "{"
    for index,(key, value) in enumerate(data_description.items()):
        fourth += f"{key}: '{value}'"
        if index == len(data_description.items()) - 1:
            continue
        fourth += ","
    fourth += "}"
    data_fields = metadata["dataFields"]
    fifth = ""
    for i, field in enumerate(data_fields):
        temp = "{"
        for index,(key, value) in enumerate(field.items()):
            temp += f"{key}: '{value}'"
            if index == len(field.items()) - 1:
                continue
            temp += ","
        temp += "}"
        fifth += temp
        if i == len(data_fields) - 1:
            continue
        fifth += ","

    insert_query_values = f"{first}," \
                          f"{second}," \
                          f"{third}," \
                          f"{fourth}," \
                          f"[{fifth}]"
    query = f"insert into XmlMetaData(file_name,xmlfileinfo,author,organization,datadescription,datafields) values('{file_name}', {insert_query_values});"
    print(query)
    session.execute(query)

def get_Data(file_name):
    query = f"select * from XmlMetaData where file_name='{file_name}'"
    result = session.execute(query)[0]
    print("File Name:", result.file_name, "\n")
    print("Author:")
    print("  Name:", result.author.name, "\n")
    print("  Email:", result.author.email, "\n")
    print("Data Description:", "\n")
    print("  Description:", result.datadescription.description, "\n")
    print("  Format:", result.datadescription.format, "\n")
    print("  Version:", result.datadescription.version, "\n")

    print("Data Fields:", "\n")
    for field in result.datafields:
        print("  Name:", field.name, "\n")
        print("  Type:", field.type, "\n")
        print("  Description:", field.description, "\n")

    print("Organization:", "\n")
    print("  Name:", result.organization.name, "\n")
    print("  Department:", result.organization.department, "\n")
    print("  Location:", result.organization.location, "\n")

    print("XML File Info:", "\n")
    print("  Filename:", result.xmlfileinfo.filename, "\n")
    print("  Filepath:", result.xmlfileinfo.filepath, "\n")
    print("  Filesize:", result.xmlfileinfo.filesize, "\n")
    print("  Created Date:", result.xmlfileinfo.createddate, "\n")
    print("  Last Modified Date:", result.xmlfileinfo.lastmodifieddate, "\n")


def read_XmlFile(xml_file_path):
    # Load the XML file
    # xml_file_path = 'path/to/your/xml/file.xml'  # Replace with the actual path

    # Read the XML file as a dictionary
    with open(xml_file_path, 'r') as xml_file:
        xml_data = xml_file.read()
        xml_dict = xmltodict.parse(xml_data)

    # Get a list of attribute names to read from the user
    selected_elements = input("Enter the fields").split(",")
    root_element = xml_dict['dataset'] #'dataset' --> 'root' element of the file
    
    for record in root_element['record']: # 'record' --> biggest container
        print("Record:")
        for element_name in selected_elements:
            if element_name in record:
                element_value = record[element_name]
                print(f"  {element_name}: {element_value}")
            else:
                print(f"  {element_name}: Not found")

if __name__ == '__main__':
    #inserting metadata - insert_Data()
    # insert_Data()
    #getting the inserted metadata - get_Data()`
    # file_name = "employees.xml"
    # get_Data(file_name)
    #Reading the data from the file
    xml_file_path = "./Data_sources/dataset.xml"
    read_XmlFile(xml_file_path)