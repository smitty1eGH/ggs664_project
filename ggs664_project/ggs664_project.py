from   datetime          import datetime
import logging
import logging.config
import pickle

import numpy
from   scipy.spatial     import ConvexHull
import shapefile
from   sqlalchemy        import create_engine
from   sqlalchemy.orm    import sessionmaker
from   sqlalchemy.sql    import text

from   model             import run__init__,District,DistrictAdjacency,Run,OutputDetail

logging.config.fileConfig('logging.conf',disable_existing_loggers=False)
logger=logging.getLogger( 'sqlalchemy').setLevel(logging.DEBUG)
logger=logging.getLogger( 'root')

SQLFILE  ='sqlite:///ggs664.sqlite'
COUNTIES ="/mnt/swap/VirgiVVnia_Administrative_Boundary_2017_SHP/VA_COUNTY"
DISTRICTS="/mnt/swap/tl_2012_51_vtd10/tl_2012_51_vtd10"
LOAD_DATA=True #False #
EXPORTDAT=True #False #
MAX_SEED =11

engine   = create_engine(SQLFILE, echo=True)
session  = sessionmaker(bind=engine)()
conn     = engine.connect()
rawconn  = engine.raw_connection()

GLO_PERM = 0  #Float meaning the perimiter
GLO_VERT = 1  #[(Float,Float),] points
gloseeds = {} #Golbal dictionary that will be called from SQLite to remember the
              #  current perimiter and vertices of the districts, so that the
              #  one with optimal characteristics can be merged.

def load_data():
    '''We ETL data from the shapefile, merging the metadata with the
         shape_points into a SQLite table.
       We then query shapefile data with a cartesian join.
       Take the points of each shapefile entry and make them into a set,
         so that we can tell if they are disjoint.
       When NOT disjoint, we deem them neighbors, and store that fact
         in the district_adjacency table.
    '''
    def shapelist2set(pointstring):
        '''POINTSTRING is a CSV list of LON/LAT pairs.
        '''
        def genpoints(pointstring):
            '''Return a generator to pull the point string values out of the source data.
               First, replace the point commas with pipes and then split on pipe.
                 We do this so that we can split the point pairs while preserving
                   the internal comma within the point.
                 Then, drop the parens and split on the internal comma.
                 Finally, make tuples of floats, if needed.
            '''
            x=pointstring.replace('[','').replace(']','').replace('), ',')|').split('|')
            for y in x:
                z=y.replace('(','').replace(')','').split(', ')
                yield tuple(( float(z[0])
                            , float(z[1])
                           ))
        ps=set()
        for x in genpoints(pointstring):
            ps.add(x)
        return ps

    def shapelist_perim(spoints):
        '''Take in a set point shape points and calculate the convex hull.
        '''
        ch=ConvexHull(numpy.array(list(spoints)))
        return ch.area #for 2D, area==perimiter


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
    for d in shapefile.Reader(DISTRICTS).shapeRecords():
        spoints=shapelist2set(str(d.shape.points))
        dd=District(STATEFP10   =d.record[0] ,COUNTYFP10=d.record[1]
                   ,VTDST10     =d.record[2] ,GEOID10   =d.record[3]
                   ,VTDI10      =d.record[4] ,NAME10    =d.record[5]
                   ,NAMELSAD10  =d.record[6] ,LSAD10    =d.record[7]
                   ,MTFCC10     =d.record[8] ,FUNCSTAT10=d.record[9]
                   ,ALAND10     =d.record[10],AWATER10  =d.record[11]
                   ,INTPTLAT10  =d.record[12],INTPTLON10=d.record[13]
                   ,shape_points=spoints
                   ,shape_perim =shapelist_perim(spoints)
                   )
        session.add(dd)
    session.commit()

    batch_size=100
    cursor    =conn.execute(SQL_ADJ)
    while True:
        rows=cursor.fetchmany(batch_size)
        if not rows: break
        for row in rows:
            a=pickle.loads(row[1])
            if not a.isdisjoint(pickle.loads(row[3])):
                print('%s\t%s' % (row[0],row[2]))
                da=DistrictAdjacency(district_left_id =row[0]
                                    ,district_right_id=row[2])
                session.add(da)
    session.commit()

def do_runs(runs :int):
    '''For each run, start with the 11 districts, there is a 'seed' entry.
       Load all of the seed entries, and add them to output_detail.
    '''
    SQL_NOM  ='''INSERT INTO seed_district(district_id)
                 SELECT      district_id
                 FROM        districts
                 ORDER BY    random()
                 LIMIT       %s;
              '''            % MAX_SEED
    SQL_NEXT_ADJ='''
        SELECT    district_right_id               as next_adjacency
               , (SELECT district_id
                  FROM   seed_district
                  WHERE  seed_district_id=%s)      as congdistrictseed
        FROM      district_adjacency
        WHERE     district_left_id
              IN (SELECT district_id               as superdistrict
                  FROM   output_detail
                  WHERE  run_id                     = %s
                     AND seed_district_id           =
                           (SELECT district_id
                            FROM   seed_district
                            WHERE  seed_district_id = %s ))
             AND  district_right_id
          NOT IN (SELECT  district_id              as already_used
                  FROM    output_detail
                  WHERE   run_id                          = %s)
        ORDER BY  RANDOM()
        LIMIT     1;
    '''
    SQL_INS_OUTP='''
        INSERT INTO output_detail(run_id,step,seed_district_id,district_id)
        VALUES     (?,?,?,?);
    '''
    conn.execute(text("DELETE FROM output_detail;"))
    session.commit()

    for r in range(runs):
        #Nominate 11 seed CongDistricts
        conn.execute(text("DELETE FROM seed_district;"))
        session.commit()
        conn.execute(SQL_NOM)
        session.commit()

        step  =0
        run   =run__init__(conn,session,step)
        contin=True
        while contin:
            for i in range(MAX_SEED):

                cursor=rawconn.cursor()
                sql   =SQL_NEXT_ADJ % (i+1,run,i+1,run)
                cursor.execute(sql)
                rows  =cursor.fetchone()

                #logger.debug('i=%s',i)
                #logger.debug(rows)
                if not rows:
                    contin=False
                    break

                conn.execute(SQL_INS_OUTP,run,step,rows[1],rows[0])
                session.commit()
            step+=1


if __name__=='__main__':
    if LOAD_DATA: load_data()
    if EXPORTDAT: export_dat()
    do_runs(10)
