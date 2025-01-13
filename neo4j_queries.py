import pandas as pd
from neo4j import GraphDatabase
from collections import defaultdict

class Neo4jConnection:
    def __init__(self, uri="bolt://localhost:7687", username="neo4j", password="12345678"):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))

    def close(self):
        self.driver.close()

    def create_graph_from_excel(self, file_path):
        # Load the Excel file into a DataFrame
        df = pd.read_excel(file_path, sheet_name="Chronological Event Development", engine='openpyxl')
        print(df.head())  # Print the first few rows to verify data loading

        # Create nodes and relationships in Neo4j
        with self.driver.session() as session:
            for _, row in df.iterrows():
                print(row)  # Print each row to verify data before uploading to Neo4j
                session.write_transaction(self._create_nodes_and_relationships, row)

    def _create_nodes_and_relationships(self, tx, row):
        tx.run("""
        MERGE (e:Event {description: $event, character: $character, sequence: $sequence, detail: $event_detail})
        MERGE (ch:Chapter {name: $chapter})
        MERGE (c:Canto {name: $canto})
        MERGE (ch)-[:BELONGS_TO]->(c)
        MERGE (e)-[:OCCURS_IN]->(ch)
        """, event=row['Event Description'], character=row['Character Name'], sequence=row['Event Sequence Number'], chapter=row['Chapter Name'], canto=row['Canto Name'], event_detail=row['Event Detail'])

    def get_character_events(self, character_name):
        with self.driver.session() as session:
            result = session.run("""
            MATCH (e:Event {character: $character_name})-[:OCCURS_IN]->(ch:Chapter)-[:BELONGS_TO]->(c:Canto)
            RETURN DISTINCT e.description AS event, e.sequence AS sequence, ch.name AS chapter, c.name AS canto, e.detail AS event_detail
            ORDER BY e.sequence
            
            """, character_name=character_name)

            events = [{"event": record["event"], "sequence": record["sequence"], "chapter": record["chapter"], "canto": record["canto"], "event_detail": record["event_detail"]} for record in result]

            return events


# This function will now sort chapters and cantos by the lowest sequence
def group_events_by_canto_and_chapter(events):
    grouped_data = defaultdict(lambda: defaultdict(list))

    # Populate the dictionary with events grouped by canto and chapter, while checking for duplicates
    for event in events:
        if event not in grouped_data[event['canto']][event['chapter']]:
            grouped_data[event['canto']][event['chapter']].append(event)

    # Sort the chapters and cantos based on the lowest sequence number in each group
    sorted_grouped_data = {
        canto: dict(sorted(chapters.items(), key=lambda chapter: min(e['sequence'] for e in chapter[1])))
        for canto, chapters in sorted(grouped_data.items(), key=lambda canto: min(e['sequence'] for e in sum(canto[1].values(), [])))
    }
    
    return sorted_grouped_data
