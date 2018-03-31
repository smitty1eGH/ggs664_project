from datetime                   import datetime
from typing                     import *

from sqlalchemy                 import create_engine
from sqlalchemy                 import Column, Integer, Float, String, DateTime, PickleType
from sqlalchemy.ext.declarative import declarative_base

Base=declarative_base()

def run__init__(conn :Any, session :Any, step :int)->int:
    '''Make a new run, entry, seed the outputdetail tablename
         with a self-referential entry so that the 'used' query doesn't
         return a null.
       Use SEEDS to make sure that the seed_district_id is meaningful.
    '''
    SQL_NEW_RUN='''
       insert into output_detail(run_id,step,seed_district_id,district_id)
       select      ?, ?, district_id, district_id
       from        seed_district;
    '''
    r=Run(run_start=datetime.now()); session.add(r); session.commit()
    conn.execute(SQL_NEW_RUN,(r.run_id,step))
    session.commit()
    return r.run_id

def compute_perimiter(seed_district_id :Any, adjacent_district_id :Any):
    '''Take in two pickled point sets;
    '''

class District(Base):
    '''Based on TIGER tl_2012_51_vtd10
       These are the ~2,300 voting districts that roll up into their
         11 Congressional districts for Virginia.
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
    shape_points = Column(PickleType) #pickled set()
    shape_perim  = Column(Float)

class DistrictAdjacency(Base):
    '''Which districts have shape_points that are NOT disjoint.
    '''
    __tablename__     ='district_adjacency'
    district_left_id  = Column(Integer, primary_key=True)
    district_right_id = Column(Integer, primary_key=True)

    def dump(f):
        '''F is an open file handle.
           Dump table in XML format.
        '''
        FRAG0='''<?xml version="1.0" encoding="UTF-8"?>
<gexf xmlns="http://www.gexf.net/1.2draft" version="1.2">
    <meta lastmodifieddate="2009-03-20">
        <creator>Gexf.net</creator>
        <description>District Adjacency Dump</description>
    </meta>
    <graph mode="static" defaultedgetype="directed">
        <nodes>
        '''
        FRAG1='</nodes><edges>'
        FRAG2='</edges></graph></gexf>'
        #SELECT '<node id="' ||  || '" label="' ||  || '"/>' FROM INNER JOIN ON;
        #SELECT '<edge id="' ||  || '" source="' ||  || '" target="' ||  || '" />' FROM INNER JOIN ON;


class SeedDistrict(Base):
    '''These are districts that are selected manually as them
         basis for building congressional districts.
       This lets the calculation start with some initial dispersion,
         so that we don't wind up choking off the corners.
    '''
    __tablename__    ='seed_district'
    seed_district_id = Column(Integer, primary_key=True)
    district_id      = Column(Integer)

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
    '''
    __tablename__    ='output_detail'
    output_detail_id = Column(Integer, primary_key=True)
    run_id           = Column(Integer)
    step             = Column(Integer)
    seed_district_id = Column(Integer)
    district_id      = Column(Integer)


if __name__=='__main__':
    '''Construct the database when invoked directly
    '''
    engine = create_engine('sqlite:///ggs664.sqlite', echo=True)
    Base.metadata.create_all(engine)
