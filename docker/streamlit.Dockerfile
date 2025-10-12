FROM python:3.12-slim
WORKDIR /app

COPY docker/requirements-streamlit.txt .
RUN pip install --no-cache-dir -r requirements-streamlit.txt

COPY . .

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore
EXPOSE 8501
CMD ["bash","-lc","streamlit run streamlit/app.py --server.port 8501 --server.address 0.0.0.0"]
