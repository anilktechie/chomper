from chomper.items import Item


item = Item(name='Jeff', job='developer')


print item.eval(Item.name == 'Jeff')
print item.eval(Item.name != 'Annie')
print item.eval(Item.job.is_in(['doctor', 'dentist', 'developer']))
