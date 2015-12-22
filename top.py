import aggre
import pdb
reload(aggre)
from aggre import Aggregator

agg = Aggregator()
#agg.store_entire_uuids()
#agg.store_entire_data()
#agg.store_entire_tag()
agg.allfiles2csv()
#pdb.run("agg.allfiles2csv()")
