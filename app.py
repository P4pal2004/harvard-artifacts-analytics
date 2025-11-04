## 🎨 Harvard Artifacts Collection: Streamlit + ETL + SQL + CSV Downloads

import pymysql
import requests
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

# -------------------- Database Connection -------------------- #
mydb = pymysql.connect(
    host="localhost",
    user="root",
    password="root",
    database="project_harvard"
)
mycursor = mydb.cursor()

API_KEY = "432b9513-8af3-4b52-b27b-f1d4235ea583"

# -------------------- Create Tables -------------------- #
mycursor.execute("""
CREATE TABLE IF NOT EXISTS artifacts_metadata (
    id INTEGER PRIMARY KEY,
    title TEXT,
    culture TEXT,
    period TEXT,
    century TEXT,
    medium TEXT,
    dimensions TEXT,
    description TEXT,
    department TEXT,
    classification TEXT,
    accessionyear INTEGER,
    accessionmethod TEXT
)
""")

mycursor.execute("""
CREATE TABLE IF NOT EXISTS artifacts_media_1 (
    objectid INTEGER,
    imagecount INTEGER,
    mediacount INTEGER,
    colorcount INTEGER,
    ranknum INTEGER,
    datebegin INTEGER,
    dateend INTEGER
)
""")

mycursor.execute("""
CREATE TABLE IF NOT EXISTS artifact_colors (
    objectid INTEGER,
    color TEXT,
    spectrum TEXT,
    hue TEXT,
    percent REAL,
    css3 TEXT
)
""")
mydb.commit()

# -------------------- Helper Functions -------------------- #
def fetch_artifacts(classification):
    """Fetch all objects from Harvard API for the classification"""
    records = []
    url = "https://api.harvardartmuseums.org/object"
    params = {"apikey": API_KEY, "classification": classification,"hasimage": 1, "size": 100, "page": 1}

    while True:
        response = requests.get(url, params=params)
        data = response.json()
        if "records" not in data:
            break
        records.extend(data["records"])
        if "info" not in data or not data["info"].get("next"):
            break
        params["page"] += 1
    return records

def split_records(records):
    """Split API records into metadata, media, and colors"""
    metadata, media, colors = [], [], []
    for i in records:
        metadata.append({
            "id": i['id'],
            "title": i.get('title'),
            "culture": i.get('culture'),
            "period": i.get('period'),
            "century": i.get('century'),
            "medium": i.get('medium'),
            "dimensions": i.get('dimensions'),
            "description": i.get('description'),
            "department": i.get('department'),
            "classification": i.get('classification'),
            "accessionyear": i.get('accessionyear'),
            "accessionmethod": i.get('accessionmethod')
        })

        media.append({
            "objectid": i['id'],
            "rank": i.get('rank'),
            "imagecount": i.get('imagecount'),
            "mediacount": i.get('mediacount'),
            "colorcount": i.get('colorcount'),
            "datebegin": i.get('datebegin'),
            "dateend": i.get('dateend')
        })

        for j in i.get('colors', []):
            colors.append({
                "objectid": i['id'],
                "hue": j.get('hue'),
                "color": j.get('color'),
                "spectrum": j.get('spectrum'),
                "percent": j.get('percent'),
                "css3": j.get('css3')
            })

    return metadata, media, colors

# -------------------- SQL Insert Functions -------------------- #
def insert_metadata(mycursor, data):
    for record in data:
        mycursor.execute("""
            INSERT INTO artifacts_metadata (id, title, culture, period, century, medium, dimensions, description, department, classification, accessionyear, accessionmethod)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, tuple(record.values()))
    mydb.commit()

def insert_media(mycursor, data):
    for record in data:
        mycursor.execute("""
            INSERT INTO artifacts_media_1 (objectid, imagecount, mediacount, colorcount, ranknum, datebegin, dateend)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, tuple(record.values()))
    mydb.commit()

def insert_colors(mycursor, data):
    for record in data:
        mycursor.execute("""
            INSERT INTO artifact_colors (objectid, color, spectrum, hue, percent, css3)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, tuple(record.values()))
    mydb.commit()

# -------------------- Streamlit UI -------------------- #
st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center; color: black;'>🎨🏛️ Harvard’s Artifacts Collection</h1>", unsafe_allow_html=True)

# Input and Collect Button
classification = st.text_input("Enter a classification (Coins, Paintings, Vessels,Photographs,Drawings,Prints)")
collect_button = st.button("Collect data")

# Option menu for SQL
menu = option_menu(None, ["Select Your Choice", "Migrate to SQL", "SQL Queries"], orientation="horizontal")

# -------------------- Collect Data -------------------- #
if collect_button:
    if classification:
        with st.spinner("Fetching data..."):
            records = fetch_artifacts(classification)
            metadata, media, colors = split_records(records)

        st.success(f"Fetched {len(records)} records!")

        c1, c2, c3 = st.columns(3)
        with c1:
            st.header("Metadata")
            st.dataframe(pd.DataFrame(metadata))
            if metadata:
                st.download_button("Download Metadata CSV",
                                   pd.DataFrame(metadata).to_csv(index=False).encode('utf-8'),
                                   f"{classification}_metadata.csv", "text/csv")
        with c2:
            st.header("Media")
            st.dataframe(pd.DataFrame(media))
            if media:
                st.download_button("Download Media CSV",
                                   pd.DataFrame(media).to_csv(index=False).encode('utf-8'),
                                   f"{classification}_media.csv", "text/csv")
        with c3:
            st.header("Colors")
            st.dataframe(pd.DataFrame(colors))
            if colors:
                st.download_button("Download Colors CSV",
                                   pd.DataFrame(colors).to_csv(index=False).encode('utf-8'),
                                   f"{classification}_colors.csv", "text/csv")
    else:
        st.error("Please enter a classification!")

# -------------------- SQL Migration -------------------- #
if menu == 'Migrate to SQL':
    mycursor.execute("SELECT DISTINCT(classification) FROM artifacts_metadata")
    classes_list = [i[0] for i in mycursor.fetchall()]

    st.subheader("Insert the collected data")
    if st.button("Insert to SQL"):
        if classification not in classes_list:
            records = fetch_artifacts(classification)
            metadata, media, colors = split_records(records)
            insert_metadata(mycursor, metadata)
            insert_media(mycursor, media)
            insert_colors(mycursor, colors)
            st.success("Data inserted successfully!")
        else:
            st.error("Classification already exists in database!")

# -------------------- SQL Queries -------------------- #
elif menu == "SQL Queries":
    st.subheader("Run Predefined SQL Queries")

    query_option = st.selectbox(
        "Select a query to run:",
        (
            "1. List all artifacts from the 11th century belonging to Byzantine culture.",
            "2. What are the unique cultures represented in the artifacts.",
            "3. List all artifacts from the Archaic Period.",
            "4. List artifact titles ordered by accession year in descending order.",
            "5. How many artifacts are there per department.",
            "6. Which artifacts have more than 1 image",
            "7. What is the average rank of all artifacts",
            "8. Which artifacts have a higher colorcount than mediacount.",
            "9. List all artifacts created between 1500 and 1600.",
            "10.How many artifacts have no media files.",
            "11.What are all the distinct hues used in the dataset?",
            "12.What are the top 5 most used colors by frequency?",
            "13.What is the average coverage percentage for each hue?",
            "14.List all colors used for a given artifact ID.",
            "15.What is the total number of color entries in the dataset?"
            "16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.",
            "17.List each artifact title with its associated hues.",
            "18.Get artifact titles, cultures, and media ranks where the period is not null.",
            '19.Find artifact titles ranked in the top 10 that include the color hue "Grey".',
            "20.How many artifacts exist per classification, and what is the average media count for each?",
            "21.Top 5 departments with most departments",
            "22.Average Color Count per Culture",
            "23.Artifacts with media but no color",
            "24.Artifacts per period and average accession year",
            "25.Artifacts missing culture information"
        ),
        index=None,
        placeholder="Select a query"
    )

    result = []
    if query_option == "1. List all artifacts from the 11th century belonging to Byzantine culture.":
        mycursor.execute("SELECT * FROM artifacts_metadata WHERE century='11th century' AND culture='Byzantine'")
        result = mycursor.fetchall()
    elif query_option == "2. What are the unique cultures represented in the artifacts.":
        mycursor.execute("SELECT DISTINCT(culture) FROM artifacts_metadata")
        result = mycursor.fetchall()
    elif query_option == "3. List all artifacts from the Archaic Period":
        mycursor.execute("SELECT * FROM artifacts_metadata WHERE period='Archaic'")
        result = mycursor.fetchall()
    elif query_option == "4. List artifact titles ordered by accession year descending":
        mycursor.execute("SELECT title, accessionyear FROM artifacts_metadata ORDER BY accessionyear DESC")
        result = mycursor.fetchall()
    elif query_option == "5. Count of artifacts per department":
        mycursor.execute("SELECT department, COUNT(*) as total_artifacts FROM artifacts_metadata GROUP BY department")
        result = mycursor.fetchall()
    elif query_option == "6. Which artifacts have more than 1 image":
        mycursor.execute("SELECT objectid, imagecount FROM artifacts_media_1 WHERE imagecount > 1")
        result = mycursor.fetchall()
    elif query_option == "7. What is the average rank of all artifacts":
        mycursor.execute("SELECT AVG(ranknum) AS average_ranknum FROM artifacts_media_1")
        result = mycursor.fetchall()
    elif query_option == "8. Which artifacts have a higher colorcount than mediacount.":
        mycursor.execute("SELECT objectid, colorcount, mediacount FROM artifacts_media_1 WHERE colorcount > mediacount")
        result = mycursor.fetchall()
    elif query_option == "9. List all artifacts created between 1500 and 1600.":
        mycursor.execute("SELECT * FROM artifacts_media_1 WHERE datebegin BETWEEN 1500 AND 1600")
        result = mycursor.fetchall()
    elif query_option == "10.How many artifacts have no media files.":
        mycursor.execute("SELECT COUNT(*) FROM artifacts_media_1 WHERE mediacount = 0")
        result = mycursor.fetchall()
    elif query_option == "11.What are all the distinct hues used in the dataset?":
        mycursor.execute("SELECT DISTINCT hue FROM artifact_colors")
        result = mycursor.fetchall()
    elif query_option == "12.What are the top 5 most used colors by frequency?":
        mycursor.execute("SELECT color, COUNT(*) AS color_count FROM artifact_colors GROUP BY color ORDER BY   color_countDESC LIMIT 5;")
        result = mycursor.fetchall()
    elif query_option == "13.What is the average coverage percentage for each hue?":
        mycursor.execute("SELECT hue, AVG(percent) AS average_percentage FROM artifact_colors GROUP BY hue")
        result = mycursor.fetchall()
    elif query_option == "14.List all colors used for a given artifact ID.":
        mycursor.execute("SELECT color, spectrum, hue, percent, css3 FROM artifact_colors WHERE objectid =  227994")
        result = mycursor.fetchall()
    elif query_option == "15.What is the total number of color entries in the dataset?":
        mycursor.execute("SELECT COUNT(*) FROM artifact_colors")
        result = mycursor.fetchall()
    elif query_option == "16.List artifact titles and hues for all artifacts belonging to the Byzantine culture.":
        mycursor.execute("SELECT artifacts_metadata.title, artifact_colors.hue FROM artifacts_metadata JOIN artifact_colors  ON artifacts_metadata.id = artifact_colors.objectid WHERE artifacts_metadata.culture = 'Byzantine'")
        result = mycursor.fetchall()
    elif query_option == "17.List each artifact title with its associated hues.":
        mycursor.execute("SELECT am.title, ac.hue FROM artifacts_metadata am JOIN artifact_colors ac ON am.id =ac.objectid")
        result = mycursor.fetchall()
    elif query_option == "18.Get artifact titles, cultures, and media ranks where the period is not null.":
        mycursor.execute("SELECT am.title, am.culture, ac.ranknum FROM artifacts_metadata am JOIN artifacts_media_1 ac ON am.id = ac.objectid WHERE am.period IS NOT NULL")
        result = mycursor.fetchall()
    elif query_option == '19.Find artifact titles ranked in the top 10 that include the color hue "Grey"':
        mycursor.execute("SELECT am.title, ac.rank, aco.hue FROM artifact_metadata am JOIN artifact_media ac ON am.id = ac.objectid JOIN artifact_colors aco ON am.id = aco.objectid WHERE aco.hue = 'Grey' ORDER BY ac.rank ASC LIMIT 10")
        result = mycursor.fetchall()       
    elif query_option == "20.How many artifacts exist per classification, and what is the average media count for each?":
        mycursor.execute("SELECT am.classification, COUNT(am.id) AS artifact_count, AVG(ac.mediacount) AS average_mediacount FROM artifacts_metadata am JOIN artifacts_media_1 ac ON am.id = ac.objectid GROUP BY am.classification ORDER BY artifact_count DESC;")
        result = mycursor.fetchall()
    elif query_option == "21.Top 5 departments with most departments":
        mycursor.execute("SELECT department, COUNT(*) AS total_artifacts FROM artifacts_metadata GROUP BY department ORDER BY total_artifacts DESC LIMIT 5")
        result = mycursor.fetchall()
    elif query_option == "22.Average Color Count per Culture":
        mycursor.execute("SELECT m.culture, COUNT(c.hue) AS total_colors FROM artifacts_metadata AS m LEFT JOIN artifact_colors AS c ON m.id = c.objectid GROUP BY m.culture ORDER BY total_colors DESC")
        result = mycursor.fetchall()
    elif query_option == "23.Artifacts with media but no color":
        mycursor.execute("SELECT m.id, m.title FROM artifacts_metadata AS m LEFT JOIN artifacts_media_1 AS me ON m.id = me.objectid LEFT JOIN artifact_colors AS c ON m.id = c.objectidWHERE me.objectid IS NOT NULL AND c.objectid IS NULL")
        result = mycursor.fetchall()
    elif query_option == "24.Artifacts per period and average accession year":
        mycursor.execute("SELECT period, COUNT(*) AS total, AVG(accessionyear) AS avg_accession_year FROM artifacts_metadata GROUP BY period")
        result = mycursor.fetchall()
    elif query_option == "25.Artifacts missing culture information":
        mycursor.execute("SELECT id, title FROM artifacts_metadata WHERE culture IS NULL OR culture = ''")
        result = mycursor.fetchall()
                                

    if result:
        df = pd.DataFrame(result, columns=[i[0] for i in mycursor.description])
        st.dataframe(df)
        st.download_button("Download Query Result CSV",
                           df.to_csv(index=False).encode('utf-8'),
                           f"{query_option.replace(' ', '_')}.csv", "text/csv")
    else:
        st.warning("No results found for this query.")

        # ---------------------------------------- #
# 🧠 LEARNER SQL QUERIES SECTION
# ---------------------------------------- #

st.markdown("---")
st.subheader("🧠 Learner SQL Queries")

learner_queries = {
    "Artifacts from the 11th century (Byzantine culture)": """
        SELECT * 
        FROM artifacts_metadata 
        WHERE century = '11th century' 
          AND culture = 'Byzantine';
    """,

    "Unique cultures represented in artifacts": """
        SELECT DISTINCT culture 
        FROM artifacts_metadata 
        WHERE culture IS NOT NULL;
    """,

    "Artifacts from the Archaic Period": """
        SELECT * 
        FROM artifacts_metadata 
        WHERE period = 'Archaic Period';
    """,

    "Artifact titles ordered by accession year (DESC)": """
        SELECT title, accessionyear 
        FROM artifacts_metadata 
        ORDER BY accessionyear DESC;
    """,

    "Artifacts count per department": """
        SELECT department, COUNT(*) AS artifact_count 
        FROM artifacts_metadata 
        GROUP BY department 
        ORDER BY artifact_count DESC;
    """
}

selected_learner_query = st.selectbox("Select a learner query:", list(learner_queries.keys()))

if st.button("Run Learner Query"):
    query = learner_queries[selected_learner_query]
    df = pd.read_sql(query, mydb)
    st.dataframe(df)  # display as table

