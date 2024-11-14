import json
import logging
import uuid

import streamlit as st
from dotenv import load_dotenv
from llama_index.core.llms import ChatMessage, MessageRole
from rag import chat_engine, vector_store

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

st.set_page_config(
    page_title="Real-time RAG with Estuary Flow", page_icon="./app/static/favicon.ico"
)

with st.sidebar:
    st.markdown("**Start building real-time data products with [Estuary Flow](https://www.estuary.dev).**")


# Load environment variables
load_dotenv()


# Streamlit UI elements
st.write("## Chat with MongoDB data")

st.image('estuary_logo.png', caption='Powered By Estuary Flow')


image_width = 300
image_height = 200


if "messages" not in st.session_state.keys():
    if "session_id" not in st.session_state.keys():
        session_id = "uuid-" + str(uuid.uuid4())

        logging.info(json.dumps({"_type": "set_session_id", "session_id": session_id}))
        st.session_state["session_id"] = session_id

    DEFAULT_MESSAGES = [
        ChatMessage(role=MessageRole.USER, content="Hi, what can you help me with?"),
        ChatMessage(
            role=MessageRole.ASSISTANT,
            content="Hi there! Ask me questions about reviews of e-commerce products!",
        ),
    ]

    chat_engine.chat_history.clear()

    for msg in DEFAULT_MESSAGES:
        chat_engine.chat_history.append(msg)

    st.session_state.messages = [
        {"role": msg.role, "content": msg.content} for msg in chat_engine.chat_history
    ]
    st.session_state.chat_engine = chat_engine
    st.session_state.vector_client = vector_store


cs = st.columns([1, 1, 1, 1], gap="large")

if prompt := st.chat_input("Your question"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    logging.info(
        json.dumps(
            {
                "_type": "user_prompt",
                "prompt": prompt,
                "session_id": st.session_state.get("session_id", "NULL_SESS"),
            }
        )
    )

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.write(message["content"])


if st.session_state.messages[-1]["role"] != "assistant":
    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            response = st.session_state.chat_engine.chat(prompt)

            sources = []

            try:
                for source in response.source_nodes:
                    full_path = source.metadata.get("path", source.metadata.get("name"))
                    if full_path is None:
                        continue
                    if "/" in full_path:
                        name = f"`{full_path.split('/')[-1]}`"
                    else:
                        name = f"`{full_path}`"
                    if name not in sources:
                        sources.append(name)
            except AttributeError:
                logging.error(
                    json.dumps(
                        {
                            "_type": "error",
                            "error": f"No source (`source_nodes`) was found in response: {str(response)}",
                            "session_id": st.session_state.get(
                                "session_id", "NULL_SESS"
                            ),
                        }
                    )
                )

            sources_text = ", ".join(sources)

            logging.info(
                json.dumps(
                    {
                        "_type": "llm_response",
                        "response": str(response),
                        "session_id": st.session_state.get("session_id", "NULL_SESS"),
                        "sources": sources,
                    }
                )
            )

            response_text = (
                response.response
            )

            st.write(response_text)

            message = {"role": "assistant", "content": response_text}
            st.session_state.messages.append(message)
