import pandas as pd
import json
import pyodbc


def read_data_from_json():
    # Creating a dataframe for data from json
    df = pd.DataFrame(
        columns=[
            "playlist_name",
            "lastModifiedDate",
            "trackName",
            "artistName",
            "albumName",
        ]
    )
    # json file is opening
    with open(r"~\Playlist1.json", encoding="utf-8") as f:  # enter your json files path
        data = json.load(f)

    # Transferring data read from json to dataframe
    for i in data["playlists"]:
        for j in i["items"]:
            new_row = {
                "playlist_name": i["name"],
                "lastModifiedDate": i["lastModifiedDate"],
                "trackName": [j][0]["track"]["trackName"],
                "artistName": [j][0]["track"]["artistName"],
                "albumName": [j][0]["track"]["albumName"],
            }
            df = df.append(new_row, ignore_index=True)
    return df


def load_playlists_to_db(df):
    # connecting to database
    conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"  # enter the driver of the database you are using.
        "Server=server_name;"  # enter your server name
        "Database=db_name;"  # enter your database name
        "Trusted_Connection=yes;"
    )

    # Inserting data to database table from dataframe with cursor
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute(
            """INSERT INTO [dbo].[playlists_tracks]
                ([playlist_name]
                ,[lastModifiedDate]
                ,[trackName]
                ,[artistName]
                ,[albumName])
                values (?,?,?,?,?)""",
            row["playlist_name"],
            row["lastModifiedDate"],
            row["trackName"],
            row["artistName"],
            row["albumName"],
        )
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    data = read_data_from_json()
    load_playlists_to_db(data)
