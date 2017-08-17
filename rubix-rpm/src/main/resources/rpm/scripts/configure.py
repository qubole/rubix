import xml.etree.cElementTree as ET

def configure():
    xml_file = "/usr/lib/hadoop/etc/hadoop/core-site.xml"
    tree = ET.ElementTree(file=xml_file)
    root = tree.getroot()
    prop = ET.Element('property')
    prop.append(ET.fromstring('<name>hadoop.cache.data.dirprefix.list</name>'))
    prop.append(ET.fromstring('<value>/var/lib/rubix/cache/data</value>'))
    root.append(prop)
    with open(xml_file, "w") as f:
        tree.write(f)

if __name__ == "__main__":
    configure()