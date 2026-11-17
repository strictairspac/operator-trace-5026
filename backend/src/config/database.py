from sqlalchemy import create_engine,text
from sqlalchemy.orm import declarative_base,sessionmaker
from src.config.settings import DATABASE_URL
engine=create_engine(DATABASE_URL,pool_pre_ping=True)
Session=sessionmaker(bind=engine)
Base=declarative_base()
def ensure_schema():
 """Metadata create plus additive columns for volumes created on older schema."""
 Base.metadata.create_all(engine)
 with engine.begin() as conn:
  cols=conn.execute(text("select column_name from information_schema.columns where table_name='traces'")).scalars().all()
  if 'mode' not in cols:
   conn.execute(text("alter table traces add column mode varchar(10) not null default 'memory'"))
