import xml.etree.ElementTree as ET
import xmltodict
import json



def xml_parser(filename):
    print(filename)
    #data = {}
    try:
        tree = ET.parse(filename)
        xml_data = tree.getroot()

        xmlstr = ET.tostring(xml_data, method='xml')
        data_dict = xmltodict.parse(xmlstr)
        return (data_dict,True)
    except Exception as e:
        response = {'status':False,'data':None,'message':e}
        return (response,False)


    # data.update(data_dict)
    #return data

# with open('data2.json','w') as f:
#     json.dump(data, f, indent=4, sort_keys=True)
