from flask import render_template, request, redirect, url_for

from neo4j_queries import Neo4jConnection, group_events_by_canto_and_chapter
import os
import pandas as pd

# Initialize Neo4j connection
neo4j_connection = Neo4jConnection(password="12345678")

UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def init_routes(app):
    app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

    @app.route('/')
    def index():
        return render_template('index.html')

    @app.route('/upload', methods=['GET', 'POST'])
    def upload_file():
        if request.method == 'POST':
            # Check if the post request has the file part
            if 'file' not in request.files:
                return "No file part in the request"
            file = request.files['file']
            # If the user does not select a file, the browser may submit an empty part without a filename
            if file.filename == '':
                return "No selected file"
            if file and allowed_file(file.filename):
                filename = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
                file.save(filename)
                neo4j_connection.create_graph_from_excel(filename)
                return "File uploaded and data loaded successfully!"
        return render_template('upload.html')

    @app.route('/search', methods=['POST'])
    def search():
        character_name = request.form['character_name']
        
        # Get sorted events from Neo4j
        events = neo4j_connection.get_character_events(character_name)
        
        # Group the events by canto and chapter based on the lowest sequence
        grouped_events = group_events_by_canto_and_chapter(events)

        # Pass `grouped_events` to the template
        return render_template('results.html', character=character_name, grouped_events=grouped_events)


    @app.teardown_appcontext
    def close_connection(exception=None):
        neo4j_connection.close()
