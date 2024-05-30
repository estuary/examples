import os
import pinecone
from dotenv import load_dotenv
from llama_index.core.chat_engine.condense_plus_context import (
    CondensePlusContextChatEngine,
)
from llama_index.llms.openai import OpenAI
from llama_index.core.llms import ChatMessage, MessageRole
from llama_index.vector_stores.pinecone import PineconeVectorStore
from llama_index.core import VectorStoreIndex

load_dotenv()


pinecone_index = pinecone.Index(
    api_key=os.environ.get("PINECONE_API_KEY"),
    host=os.environ.get("PINECONE_HOST"),
)

vector_store = PineconeVectorStore(pinecone_index=pinecone_index, namespace="Support_Requests", text_key="flow_document")

retriever = VectorStoreIndex.from_vector_store(vector_store).as_retriever(
    similarity_top_k=5
)

llm = OpenAI(model="gpt-3.5-turbo")

chat_engine = CondensePlusContextChatEngine.from_defaults(
    retriever=retriever,
    system_prompt="""You are a helpful chat bot that answers users questions based on a database on customer support 
    requests and complaints. If you receive an unrelated question; just say "I don't know".
    """,
    verbose=True,
    llm=llm,
)
