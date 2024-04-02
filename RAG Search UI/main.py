import os
from typing import List
import traceback
import uuid
import time
from langchain_community.vectorstores import Qdrant
from langchain.chains import (
    ConversationalRetrievalChain,
)
from langchain.chat_models import ChatOpenAI

from langchain.docstore.document import Document
from langchain.memory import ChatMessageHistory, ConversationBufferMemory
from langchain_community.embeddings.sentence_transformer import (
    SentenceTransformerEmbeddings,
)

from qdrant_client import QdrantClient, AsyncQdrantClient
from quixstreams import Application
from quixstreams.models.serializers import (
    JSONSerializer,
    SerializationContext,
)
import logging
import chainlit as cl

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

openai_apikey = os.environ['OPENAI_API_KEY']
collection = os.environ['collectionname']
#TEST COLLECTIONS: "quix-techdocs-no0_5b_1kchars" # "quix-techdocs-no0_5b"
embeddings = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")
outputtopicname = os.environ["output"]

@cl.on_chat_start
async def on_chat_start():
    client = QdrantClient(
                    url="https://620342be-1e5e-401c-98da-42bcaddaed57.us-east4-0.gcp.cloud.qdrant.io:6333",
                    api_key=os.environ['QDRANT_APIKEY'],
                    timeout=100,
                    grpc_port=6334,
                    prefer_grpc=True
                )
    # client = QdrantClient(path="./qdrant-db-buffer")
    
    aclient = AsyncQdrantClient(
                    url="https://620342be-1e5e-401c-98da-42bcaddaed57.us-east4-0.gcp.cloud.qdrant.io:6333",
                    api_key=os.environ['QDRANT_APIKEY'],
                    timeout=100,
                    grpc_port=6334,
                    prefer_grpc=True
                )
    # aclient = AsyncQdrantClient(path="./qdrant-db-buffer")

    vectorstore = Qdrant(
        async_client=aclient,
        client=client,
        collection_name=collection,
        embeddings=embeddings,
    )
    docs_retriever = vectorstore.as_retriever()
    message_history = ChatMessageHistory()

    memory = ConversationBufferMemory(
        memory_key="chat_history",
        output_key="answer",
        chat_memory=message_history,
        return_messages=True,
    )

    # Create a chain that uses the Qdrant vector store
    chain = ConversationalRetrievalChain.from_llm(
        ChatOpenAI(model_name="gpt-4", temperature=0, streaming=True),
        chain_type="stuff",
        retriever=docs_retriever,
        memory=memory,
        return_source_documents=True,
    )

    cl.user_session.set("chain", chain)


searchquery = ""
answer = ""
source_documents = []
text_elements = ""

@cl.on_message
async def main(message: cl.Message):
    try:
        chain = cl.user_session.get("chain")  # type: ConversationalRetrievalChain
        cb = cl.AsyncLangchainCallbackHandler()

        searchquery = message.content

        res = await chain.acall(message.content, callbacks=[cb])
        answer = res["answer"]
        source_documents = res["source_documents"]  # type: List[Document]

        # Log the source documents to see if any have None as page_content
        logger.info(f"Source documents: {source_documents}")

        text_elements = []  # type: List[cl.Text]

        if source_documents:
            for source_idx, source_doc in enumerate(source_documents):
                if source_doc.page_content is None:
                    logger.error(f"Document at index {source_idx} has None as page_content")
                source_name = f"source_{source_idx}"
                text_elements.append(
                    cl.Text(content=source_doc.page_content, name=source_name)
                )
            source_names = [text_el.name for text_el in text_elements]

            if source_names:
                answer += f"\nSources: {', '.join(source_names)}"
            else:
                answer += "\nNo sources found"

        await cl.Message(content=answer, elements=text_elements).send()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        logger.debug(traceback.format_exc())
        # Handle the error appropriately, possibly sending a message to the user

#### START QUIX STUFF ######
    app = Application.Quix()
    # app = Application(broker_address='localhost:19092')
    serializer = JSONSerializer()
    topic = app.topic(name=outputtopicname, value_serializer=serializer)

    source_documents_serializable = [
    {
        "page_content": doc.page_content,
        "metadata": doc.metadata
    }
    for doc in source_documents
    ]

    # load_dotenv("./quix_vars.env")
    print(f"Producing to output topic: {outputtopicname}...\n\n")
    serialize = JSONSerializer()
    idcounter = 0
    with app.get_producer() as producer:
        idcounter = idcounter + 1
        doc_id = idcounter
        doc_key = f"A{'0'*(10-len(str(doc_id)))}{doc_id}"
        doc_uuid = str(uuid.uuid4())
        value = {
            "Timestamp": time.time_ns(),
            "query": searchquery,
            "answer": answer,
            "matching_docs": source_documents_serializable
            }

        print(f"Producing value: {value}...")
        # with current functionality, we need to manually serialize our data
        serialized = topic.serialize(
            key=doc_key,
            value=value,
            headers={**serializer.extra_headers, "uuid": doc_uuid},
        )

        producer.produce(
            topic=topic.name,
            headers=serialized.headers,
            key=serialized.key,
            value=serialized.value,
            )

    print("ingested quix docs")