# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 10:34:41 2017

@author: Cassandra
"""

import xml.dom.minidom as dom

import datetime, csv


# dictionary of all entities with code as key and value is the entity
dict_entities = {}

# dictionary of all properties with code as key and the name as value
dict_properties = {}

# dictionary of all qualifiers with code as key and the name as value
dict_qualifiers = {}

# dictionary of all association with code as key and the name as value
dict_associations = {}

# dictionary with all ndf-rt relationships
dict_relationships = {}

# dictionary with entity as key and and value a list of dictionaries of the different nodes
dict_entity_to_nodes = {}

# dictionary entity to file
dict_entity_to_file = {}

# dictionary relationship to file
dict_rela_to_file = {}

'''
add all information into a given dictionary
'''


def extract_and_add_info_into_dictionary(dictionary, terminology, element):
    element_list = terminology.getElementsByTagName(element)
    for combined_element in element_list:
        name = combined_element.getElementsByTagName('name')[0].childNodes[0].nodeValue
        code = combined_element.getElementsByTagName('code')[0].childNodes[0].nodeValue
        dictionary[code] = name


# cypher file
cypher_file = open('cypher_file.cypher', 'w')


def load_ndf_rt_xml_inferred_in():
    print(datetime.datetime.utcnow())
    tree = dom.parse("NDFRT_Public_2018.02.05_TDE.xml")
    print(datetime.datetime.utcnow())

    terminology = tree.documentElement

    # save all kindDef (Entities) in a dictionary with code and name
    extract_and_add_info_into_dictionary(dict_entities, terminology, 'kindDef')
    properties_of_node = ['code', "name", "nui", "properties", "association"]
    for code, entity_name in dict_entities.items():
        file_name = 'results/'+entity_name + '_file.csv'
        entity_file = open(file_name, 'w')
        csv_writer = csv.writer(entity_file, delimiter=',', quotechar='"', lineterminator='\n')
        csv_writer.writerow(properties_of_node)
        dict_entity_to_file[code] = csv_writer
        query = '''USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM "file:/home/cassandra/Dokumente/Project/master_database_change/import_into_Neo4j/ndf_rt/%s" AS line Create (n: NDF_RT_''' + entity_name + '{'
        print(query)
        print(file_name)
        query = query % (file_name)
        for propertie in properties_of_node:
            if not propertie in ['properties','association']:
                query += propertie + ':line.' + propertie + ','
            else:
                query += propertie + ':split(line.' + propertie + ',"|"),'
        query = query[:-1] + '});\n'
        cypher_file.write(query)
        #create index on code of all ndf-rt enities
        query='Create Constraint On (node:NDF_RT_' + entity_name+') Assert node.code Is Unique; \n'
        cypher_file.write(query)

    # save for all properties the code and name in a dictionary
    extract_and_add_info_into_dictionary(dict_properties, terminology, 'propertyDef')

    # save all qualifier in a dictionary with code and name
    extract_and_add_info_into_dictionary(dict_qualifiers, terminology, 'qualifierDef')

    # save all association in a dictionary
    extract_and_add_info_into_dictionary(dict_associations, terminology, 'associationDef')

    # save all association in a dictionary and generate the different cypher queries for the different relationships
    element_list = terminology.getElementsByTagName('roleDef')
    rela_info_list=['start_node','end_node']
    for combined_element in element_list:
        name = combined_element.getElementsByTagName('name')[0].childNodes[0].nodeValue
        name=name.split(' {')[0]
        code = combined_element.getElementsByTagName('code')[0].childNodes[0].nodeValue
        dict_relationships[code] = name

        # this part is for generating and adding the cypher queries
        start_node_code = combined_element.getElementsByTagName('domain')[0].childNodes[0].nodeValue
        end_node_code = combined_element.getElementsByTagName('range')[0].childNodes[0].nodeValue

        file_name ='results/'+ name + '_file.csv'
        entity_file = open(file_name, 'w')
        csv_writer = csv.writer(entity_file, delimiter=',', quotechar='"',lineterminator='\n')
        csv_writer.writerow(rela_info_list)
        dict_rela_to_file[code] = csv_writer
        query = '''USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM "file:/home/cassandra/Dokumente/Project/master_database_change/import_into_Neo4j/ndf_rt/%s" AS line Match (start: NDF_RT_''' + dict_entities[start_node_code] + '''{code:line.'''+ rela_info_list[0]+ '''}), (end: NDF_RT_''' + dict_entities[end_node_code] + '''{code:line.'''+rela_info_list[1]+'''}) Create (start)-[:%s]->(end);\n'''
        print(query)
        query=query% (file_name, name)

        cypher_file.write(query)

    # get all important concepts
    concepts = terminology.getElementsByTagName('conceptDef')
    for concept in concepts:
        # gete information about node
        entity_code=concept.getElementsByTagName('kind')[0].childNodes[0].nodeValue
        name = concept.getElementsByTagName('name')[0].childNodes[0].nodeValue
        code = concept.getElementsByTagName('code')[0].childNodes[0].nodeValue
        ndf_rt_id = concept.getElementsByTagName('id')[0].childNodes[0].nodeValue

        # go through all possible Role (Relationships) and add the to the different csv files
        definitionRoles = concept.getElementsByTagName('definingRoles')[0]
        if definitionRoles.hasChildNodes() == True:
            roles = definitionRoles.getElementsByTagName('role')
            for role in roles:
                rela_code=role.getElementsByTagName('name')[0].childNodes[0].nodeValue
                dict_rela_to_file[rela_code].writerow([code,role.getElementsByTagName('value')[0].childNodes[0].nodeValue])

        # go through all properties of this drug and generate a list of string
        prop = concept.getElementsByTagName('properties')[0]
        properties = prop.getElementsByTagName('property')
        properties_list = []
        for proper in properties:
            name_property = proper.getElementsByTagName('name')[0].childNodes[0].nodeValue
            value = proper.getElementsByTagName('value')[0].childNodes[0].nodeValue
            value = value.replace('"', '\'')
            text = dict_properties[name_property] + ':' + value
            properties_list.append(text)

        properties_string='|'.join(properties_list).encode("utf-8")

        # go through association of this drug and generate a list of string
        association_list = []
        if len(concept.getElementsByTagName('associations')) > 0:
            associat = concept.getElementsByTagName('associations')[0]
            associations = associat.getElementsByTagName('association')

            if len(associations) > 0:

                for association in associations:
                    name_association = association.getElementsByTagName('name')[0].childNodes[0].nodeValue
                    value = association.getElementsByTagName('value')[0].childNodes[0].nodeValue
                    text = dict_associations[name_association] + ':' + value
                    association_list.append(text)
        association_string='|'.join(association_list).encode("utf-8")

        dict_entity_to_file[entity_code].writerow([code, name, ndf_rt_id, properties_string, association_string])


def main():
    # start the function to load in the xml file and save the importen values in list and dictionaries
    print('#############################################################')
    print(datetime.datetime.utcnow())
    print('load in the xml data')
    load_ndf_rt_xml_inferred_in()


    print('#############################################################')
    print(datetime.datetime.utcnow())


if __name__ == "__main__":
    # execute only if run as a script
    main()
