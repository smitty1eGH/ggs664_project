import shapefile
from   sqlalchemy     import create_engine
from   sqlalchemy.orm import sessionmaker
from   sqlalchemy.sql import text
from   model          import District,DistrictAdjacency

COUNTIES ="/mnt/swap/Virginia_Administrative_Boundary_2017_SHP/VA_COUNTY"
DISTRICTS="/mnt/swap/tl_2012_51_vtd10/tl_2012_51_vtd10"
LOAD_DATA=False #True
SQL_ADJ  ='''SELECT C.a_district_id, C.a_shape_points
                  , C.b_district_id, C.b_shape_points
             FROM  (SELECT A.district_id  as a_district_id
                         , A.shape_points as a_shape_points
                         , B.district_id  as b_district_id
                         , B.shape_points as b_shape_points
                    FROM   districts A
                         , districts B) C
             WHERE  C.a_district_id != C.b_district_id;
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

engine =create_engine('sqlite:///ggs664.sqlite', echo=True)
conn   =engine.connect()
session=sessionmaker(bind=engine)() #instantiate the class, already

def load_data():
    '''We ETL data from the shapefile, merging the metadata with the
         shape_points into a SQLite table.
       We then query shapefile data with a cartesian join.
       Take the points of each shapefile entry and make them into a set,
         so that we can tell if they are disjoint.
       When NOT disjoint, we deem them neighbors, and store that fact
         in the district_adjacency table.
    '''
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

    batch_size=100
    cursor=conn.execute(SQL_ADJ)
    while True:
        rows=cursor.fetchmany(batch_size)
        if not rows: break
        for row in rows:
            a=shapelist2set(row[1])
            if not a.isdisjoint(shapelist2set(row[3])):
                print('%s\t%s' % (row[0],row[2]))
                da=DistrictAdjacency(district_left_id =row[0]
                                    ,district_right_id=row[2])
                session.add(da)
    session.commit()

    #TODO: nominate 11 seed CongDistricts

def do_runs():
    '''For each run, start with the 11 districts, there is a 'seed' entry.
       Load all of the seed entries, and add them to output_detail.

    '''

if __name__=='__main__':
    if LOAD_DATA: load_data()
    do_runs()
