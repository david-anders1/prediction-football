import uuid
from sqlalchemy import Integer, String, Column, Date, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Result(Base):
    __tablename__ = 'results'
    
    id_match = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    division = Column(String, nullable=True) 
    date = Column(Date, nullable=True)
    home_team = Column(String, nullable=True)
    away_team = Column(String, nullable=True)
    ft_home_team_goals = Column(Integer, nullable=True)
    ft_away_team_goals = Column(Integer, nullable=True)
    ft_result = Column(String, nullable=True)
    ht_result = Column(String, nullable=True)
    ht_home_team_goals = Column(Integer, nullable=True)
    ht_away_team_goals = Column(Integer, nullable=True)


class MatchStatistics(Base):
    __tablename__ = 'match_statistics'
    
    id_match = Column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    division = Column(String)
    time = Column(Date)
    home_team = Column(String)
    away_team = Column(String)
    attendance = Column(Integer)
    referee = Column(String)
    home_team_shots = Column(Integer)
    away_team_shots = Column(Integer)
    home_team_shots_on_target = Column(Integer)
    away_team_shots_on_target = Column(Integer)
    home_team_hit_woodwork = Column(Integer)
    away_team_hit_woodwork = Column(Integer)
    home_team_corners = Column(Integer)
    away_team_corners = Column(Integer)
    home_team_fouls_committed = Column(Integer)
    away_team_fouls_committed = Column(Integer)
    home_team_offsides = Column(Integer)
    away_team_offsides = Column(Integer)
    home_team_yellow_cards = Column(Integer)
    away_team_yellow_cards = Column(Integer)
    home_team_red_cards = Column(Integer)
    away_team_red_cards = Column(Integer)
    home_team_bookings_points = Column(Integer)
    away_team_bookings_points = Column(Integer)
