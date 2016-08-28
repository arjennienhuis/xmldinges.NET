from lxml import etree

WOONPLAATS = '{http://www.kadaster.nl/schemas/imbag/lvc/v20090901}Woonplaats'

f = open('test.xml', 'rb')
context = etree.iterparse(f, tag=WOONPLAATS)

for action, elem in context:
    print("%s: %s" % (action, elem.tag))
    elem.clear()
root = context.root

