from quixstreams import Application
from sentence_transformers import SentenceTransformer
from qdrant_client import models, QdrantClient
import os

uselocal = False

if uselocal == True:
  qdrant = QdrantClient(path="./qdrant-db")
else:
  qdrant = QdrantClient(
      url="https://3e6c6102-292f-4c3e-af0a-7cbf4de6141e.europe-west3-0.gcp.cloud.qdrant.io:6333",
      api_key=os.environ['qdrant_apikey'],
      grpc_port=6334,
      prefer_grpc=True,
      timeout=100
  )

encoder = SentenceTransformer('all-MiniLM-L6-v2') # Model to create embeddings
collection = os.environ['collectionname']
createcollection = os.environ['createcollection'] == "True"
count = 0

if createcollection == True:
  # Create collection to store items
  qdrant.recreate_collection(
      collection_name=collection,
      vectors_config=models.VectorParams(
          size=encoder.get_sentence_embedding_dimension(), # Vector size is defined by used model
          distance=models.Distance.COSINE
      )
  )

# Get collection to store items
qdrant.get_collection(
    collection_name=collection,
)

# Define the ingestion function
def ingest_vectors(row):
  global count
  #print(f"processing message: {row}")
  
  if len(row['page_content']) < 1:
    print("Detected Empty Page Content")
    row['page_content'] = "No content"

  metadata = row['metadata']

  single_record = models.PointStruct(
    id=metadata['uuid'],
    vector=row['embeddings'],
    payload=row
    )

  qdrant.upload_points(
      collection_name=collection,
      points=[single_record]
    )

  print(f'Ingested vector number {count} with entry id: "{metadata["uuid"]}"...')
  count = count + 1

offsetlimit = 125

# def on_message_processed(topic, partition, offset):
#     if offset > offsetlimit:
#         app.stop()

app = Application.Quix(
    consumer_group="qdrant-ingestion-v3",
    auto_offset_reset="earliest",
    #on_message_processed=on_message_processed,
    consumer_extra_config={"allow.auto.create.topics": "true", "max.poll.interval.ms": 900000 },
    producer_extra_config={"allow.auto.create.topics": "true"},
)

# Define an input topic with JSON deserializer
input_topic = app.topic(os.environ['input'], value_deserializer="json") # Merlin.. i updated this for you

# Initialize a streaming dataframe based on the stream of messages from the input topic:
sdf = app.dataframe(topic=input_topic)

# INGESTION HAPPENS HERE
### Trigger the embedding function for any new messages(rows) detected in the filtered SDF
sdf = sdf.update(lambda row: ingest_vectors(row))
app.run(sdf)