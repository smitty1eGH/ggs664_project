import shapefile
from   sqlalchemy     import create_engine
from   sqlalchemy.orm import sessionmaker
from   model          import District

COUNTIES ="/mnt/swap/Virginia_Administrative_Boundary_2017_SHP/VA_COUNTY"
DISTRICTS="/mnt/swap/tl_2012_51_vtd10/tl_2012_51_vtd10"

engine   =create_engine('sqlite:///ggs664.sqlite', echo=True)
session  =sessionmaker(bind=engine)() #just instantiate the class already

for d in shapefile.Reader(DISTRICTS).shapeRecords():
    dd=District(STATEFP10 =d.record[0] ,COUNTYFP10=d.record[1]
               ,VTDST10   =d.record[2] ,GEOID10   =d.record[3]
               ,VTDI10    =d.record[4] ,NAME10    =d.record[5]
               ,NAMELSAD10=d.record[6] ,LSAD10    =d.record[7]
               ,MTFCC10   =d.record[8] ,FUNCSTAT10=d.record[9]
               ,ALAND10   =d.record[10],AWATER10  =d.record[11]
               ,INTPTLAT10=d.record[12],INTPTLON10=d.record[13]
               )
    session.add(dd)
session.commit()
