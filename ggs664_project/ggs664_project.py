import shapefile
from   sqlalchemy     import create_engine
from   sqlalchemy.orm import sessionmaker
from   sqlalchemy.sql import text
from   model          import District

COUNTIES ="/mnt/swap/Virginia_Administrative_Boundary_2017_SHP/VA_COUNTY"
DISTRICTS="/mnt/swap/tl_2012_51_vtd10/tl_2012_51_vtd10"
LOAD_DATA=False
SQL_ADJ  ='''SELECT C.a_district_id, C.a_shape_points, C.b_district_id, C.b_shape_points
             FROM  (SELECT A.district_id as a_district_id, A.shape_points as a_shape_points
                         , B.district_id as b_district_id, B.shape_points as b_shape_points
                    FROM   districts A, districts B) C
             WHERE C.a_district_id != C.b_district_id;
          '''
def shapelist2set(pointstring):
    '''POINTSTRING is a CSV list of LON/LAT pairs.
    '''
    #def genpoints(pointstring):
    #    '''Return a generator to pull the point string values out of the source data.
    #       First, replace the point commas with pipes and then split on pipe.
    #       #  Then, drop the parens and split on the internal comma.
    #       #  Finally, make tuples of floats, if needed.
    #       #  y=x.replace('(','').replace(')','').split(', ')
    #       #  print(tuple((float(y[0]),float(y[1]))))
    #    '''
    ps=set()
    for x in pointstring.replace('[','').replace(']','').replace('), ',')|').split('|'):
        ps.add(x)
    return ps

engine=create_engine('sqlite:///ggs664.sqlite', echo=True)
conn  =engine.connect()

if LOAD_DATA:
    session=sessionmaker(bind=engine)() #just instantiate the class already
    for d in shapefile.Reader(DISTRICTS).shapeRecords():
        dd=District(STATEFP10   =d.record[0] ,COUNTYFP10=d.record[1]
                   ,VTDST10     =d.record[2] ,GEOID10   =d.record[3]
                   ,VTDI10      =d.record[4] ,NAME10    =d.record[5]
                   ,NAMELSAD10  =d.record[6] ,LSAD10    =d.record[7]
                   ,MTFCC10     =d.record[8] ,FUNCSTAT10=d.record[9]
                   ,ALAND10     =d.record[10],AWATER10  =d.record[11]
                   ,INTPTLAT10  =d.record[12],INTPTLON10=d.record[13]
                   ,shape_points=str(d.shape.points)
                   )
        session.add(dd)
    session.commit()

cur=conn.execute(SQL_ADJ)
#print(shapelist2set(ASDF))
try:
    while cur:
        x=cur.fetchone()
        s0=shapelist2set(x[1])
        s1=shapelist2set(x[3])
        if not s0.isdisjoint(s1):
            print('%s %s' % (x[0],x[2]))
except TypeError:
    pass
