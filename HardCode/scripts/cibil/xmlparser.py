import xml.etree.ElementTree as ET
import xmltodict


def xml_parser(filename):
    try:
        tree = ET.parse(filename)
        xml_data = tree.getroot()

        xmlstr = ET.tostring(xml_data, method='xml')
        data_dict = xmltodict.parse(xmlstr)
        return data_dict, True
    except Exception as e:
        response = {'status': False, 'data': None, 'message': str(e)}
        return response, False
