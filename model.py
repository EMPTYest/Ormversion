from sqlalchemy import create_engine

from sqlalchemy.orm import relationship, declarative_base

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.sql import func
import psycopg2
import time
Base = declarative_base()

class Article(Base):
    __tablename__ = 'Article'
    __table_args__ = {'schema': 'public'}

    article_id = Column(Integer, primary_key=True)
    title = Column(String(50), nullable=False)
    annotation = Column(String(50), nullable=False)
    doi = Column(String(50), nullable=False)
    publicationdate = Column(String(50), nullable=False)

    Topics = relationship("Topic", secondary="TopicArticle", back_populates="Articles")
    Authors = relationship("Author",secondary="Article_Author", back_populates="Articles")

class Topic(Base):
    __tablename__ = 'Topic'
    __table_args__ = {'schema': 'public'}

    topic_id = Column(Integer, primary_key=True)
    topicname = Column(String(20), nullable=False)
    topicgeneralspecification = Column(String(20), nullable=False)


    Articles = relationship("Article", secondary="TopicArticle", back_populates="Topics")

class TopicArticle(Base):
    __tablename__ = 'TopicArticle'
    __table_args__ = {'schema': 'public'}

    topicarticle_id = Column(Integer, primary_key=True)
    article_id = Column(Integer,ForeignKey(Article.article_id), nullable=False)
    topic_id = Column(Integer,ForeignKey(Topic.topic_id), nullable=False)





class Author(Base):
    __tablename__ = 'Author'
    __table_args__ = {'schema': 'public'}

    author_id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False)
    affiliation = Column(String(50), nullable=False)
    email = Column(String(50), nullable=False)

    Articles = relationship("Article", secondary="Article_Author", back_populates="Authors")


class Article_Author(Base):
    __tablename__ = 'Article_Author'
    __table_args__ = {'schema': 'public'}

    article_author_id = Column(Integer, primary_key=True)
    author_id = Column(Integer,ForeignKey(Author.author_id), nullable=False)
    article_id = Column(Integer,ForeignKey(Article.article_id), nullable=False)




class Model:

    def __init__(self):
        self.engine = create_engine('postgresql://postgres:1111@localhost:5432/postgres')
        Base.metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

        self.conn = psycopg2.connect(
            dbname='postgres',
            user='postgres',
            password='1111',
            host='localhost',
            port=5432
        )

    def get_all_tables(self):
        c = self.conn.cursor()
        c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        return c.fetchall()

    def add_data(self, table_name, columns, val):
        session = self.Session()
        try:
            table_class = globals()[table_name]
            record = table_class(**dict(zip(columns, val)))
            session.add(record)
            session.commit()
            return 1
        except Exception as e:
            print(e)
            session.rollback()
            return 0
        finally:
            session.close()




    def update_data(self, table_name, column, record_id, new_value):
        session = self.Session()
        try:
            model = globals()[table_name]
            obj = session.query(model).get(record_id)
            if not obj:
                return 2  # Record not found

            setattr(obj, column, new_value)
            session.commit()
            return 1  # Successfully updated
        except Exception as e:
            session.rollback()
            return f"Error updating entry: {e}"
        finally:
            session.close()

    def delete_data(self, table_name, record_id):
        session = self.Session()
        try:
            model = globals()[table_name]
            obj = session.query(model).get(record_id)
            if not obj:
                return 2  # Record not found

            session.delete(obj)
            session.commit()
            return 1  # Successfully deleted
        except Exception as e:
            session.rollback()
            return f"Error deleting entry: {e}"
        finally:
            session.close()

    def search_data(self, table, stable, row_name, row_data, group_name):
        session = self.Session()
        try:
            table_model = globals()[table]
            stable_model = globals()[stable]

            query = session.query(
                getattr(table_model, group_name).label(group_name),
                func.count(getattr(stable_model, f"{stable.lower()}_id")).label("total_articles")
            ).join(stable_model).filter(getattr(table_model, row_name) == row_data
            ).group_by(getattr(table_model, group_name)).order_by(
                func.count(getattr(stable_model, f"{stable.lower()}_id")).desc())

            results = query.all()
            for result in results:
                print(f"|{result[0]}|{result[1]}|")
            return 1
        except Exception as e:
            return f"Error searching data: {e}"
        finally:
            session.close()

    def print_table(self, table):
        session = self.Session()
        try:
            model = globals()[table]
            records = session.query(model).all()
            for record in records:
                print(record)
            return 1
        except Exception as e:
            return f"Error printing table: {e}"
        finally:
            session.close()

    def generate_data(self, table_name, count):

        c = self.conn.cursor()
        try:
            c.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,))
            columns_info = c.fetchall()


            id_column = f'{table_name.lower()}_id'
            name=''

            for i in range(count):
                insert_query = f'INSERT INTO "public"."{table_name}" ('
                select_subquery = ""

                for column_info in columns_info:
                    column_name = column_info[0]
                    column_type = column_info[1]

                    if column_name == id_column:
                        c.execute(f'SELECT max("{id_column}") FROM "public"."{table_name}"')
                        max_id = c.fetchone()[0] or 0
                        select_subquery += f'{max_id + 1},'
                    elif column_name == "name":
                        c.execute(f"SELECT CASE FLOOR(1 + RANDOM() * 5)::INT  WHEN 1 THEN 'Olexandr' WHEN 2 THEN 'Maria' WHEN 3 THEN 'Ivan' WHEN 4 THEN 'Anna' WHEN 5 THEN 'Dmitro' END ")
                        name = c.fetchone()[0]
                        select_subquery += f"'{name}',"
                    elif column_name == "email":
                        select_subquery += f"'{name}@example.com',"
                    elif column_name.endswith('_id'):
                        related_table_name = column_name[:-3].capitalize()
                        c.execute(f'SELECT {related_table_name.lower()}_id FROM "public"."{related_table_name}" ORDER '
                                  f'BY RANDOM() LIMIT 1')
                        related_id = c.fetchone()[0]

                        select_subquery += f'{related_id},'
                    elif column_type == 'integer':
                        select_subquery += f'trunc(random()*100)::INT,'
                    elif column_type == 'character varying':
                        c.execute(
                            f"SELECT CASE FLOOR(1 + RANDOM() * 2)::INT  WHEN 1 THEN 'some text' WHEN 2 THEN 'not some text'  END ")
                        name1 = c.fetchone()[0]
                        select_subquery += f"'{name1}',"
                    elif column_type == 'timestamp with time zone':
                        c.execute(f"SELECT timestamp with time zone '2022-01-01 08:30:00+03' + random() * (timestamp with time zone '2022-10-01 08:30:00+03' - timestamp with time zone '2022-01-01 20:30:00+03')")
                        name = c.fetchone()[0]
                        select_subquery += f"'{name}',"
                    else:
                        continue

                    insert_query += f'"{column_name}",'

                insert_query = insert_query.rstrip(',') + f') VALUES ({select_subquery[:-1]})'

                c.execute(insert_query)

            self.conn.commit()
            return 1
        except Exception as e:
            return 2


