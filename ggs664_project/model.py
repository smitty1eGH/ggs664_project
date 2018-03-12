from sqlalchemy                 import create_engine
from sqlalchemy                 import Column, Integer, String
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
    shape_points = Column(String)

class DistrictAdjacency(Base):
    '''Which districts have shape_points that are not disjoint.
    '''
    __tablename__     ='districtadjacency'
    district_left_id  = Column(Integer, primary_key=True)
    district_right_id = Column(Integer, primary_key=True)

if __name__=='__main__':
    '''Construct the database when invoked directly
    '''
    engine = create_engine('sqlite:///ggs664.sqlite', echo=True)
    Base.metadata.create_all(engine)
