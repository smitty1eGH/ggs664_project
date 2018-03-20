from sqlalchemy                 import create_engine
from sqlalchemy                 import Column, Integer, String, DateTime, PickleType
from sqlalchemy.ext.declarative import declarative_base

Base   =declarative_base()

class District(Base):
    '''Based on TIGER tl_2012_51_vtd10
    '''
    __tablename__='districts'
    district_id  = Column(Integer, primary_key=True)
    STATEFP10    = Column(String)
    COUNTYFP10   = Column(String)
    VTDST10      = Column(String)
    GEOID10      = Column(String)
    VTDI10       = Column(String)
    NAME10       = Column(String)
    NAMELSAD10   = Column(String)
    LSAD10       = Column(String)
    MTFCC10      = Column(String)
    FUNCSTAT10   = Column(String)
    ALAND10      = Column(String)
    AWATER10     = Column(String)
    INTPTLAT10   = Column(String)
    INTPTLON10   = Column(String)
    shape_points = Column(PickleType)

class DistrictAdjacency(Base):
    '''Which districts have shape_points that are NOT disjoint.
    '''
    __tablename__     ='districtadjacency'
    district_left_id  = Column(Integer, primary_key=True)
    district_right_id = Column(Integer, primary_key=True)

class CongDistrictSeed(Base):
    '''These are districts that are selected manually as them
         basis for building congressional districts.
       This lets the calculation start with some initial dispersion,
         so that we don't wind up choking off the corners.
    INSERT INTO congdistrictseed(district_id)
    SELECT      district_id
    FROM        districts
    ORDER BY    random()
    LIMIT 11;
    '''
    __tablename__         ='congdistrictseed'
    cong_district_seed_id = Column(Integer, primary_key=True)
    district_id           = Column(Integer)

class Run(Base):
    '''We manage the run duration and get a key here.
    '''
    __tablename__ = 'run'
    run_id        = Column(Integer, primary_key=True)
    run_start     = Column(DateTime)
    run_stop      = Column(DateTime)

class OutputDetail(Base):
    '''Iteratively populated table. For each run, the CongDistricts will
         grow from their seeds.
       We will randomly pick a voting distcrict
         that is adjacent to the merged districts from the OutputDetail,
         but has not already been taken.

       INSERT INTO outputdetail( run_id
                               , step
                               , cong_district_seed_id
                               , district_id
                               )
       SELECT      ?, ?, ?, district_right_id
       FROM        districtadjacency
       WHERE       district_left_id IN      (SELECT district_id
                                             FROM   outputdetail
                                             WHERE  run_id                 = ?
                                                AND cong_distcrict_seed_id = ?)
               AND district_right_id NOT IN (SELECT district_id
                                             FROM   outputdetail
                                             WERE   run_id                 = ?)
       ORDER BY    RANDOM()
       LIMIT 1;
    '''
    __tablename__         ='outputdetail'
    output_detail_id      = Column(Integer, primary_key=True)
    run_id                = Column(Integer)
    step                  = Column(Integer)
    cong_district_seed_id = Column(Integer)
    district_id           = Column(Integer)


if __name__=='__main__':
    '''Construct the database when invoked directly
    '''
    engine = create_engine('sqlite:///ggs664.sqlite', echo=True)
    Base.metadata.create_all(engine)
