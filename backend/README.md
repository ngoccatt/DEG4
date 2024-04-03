# Backend source code
## There are two ways to start the Flask server. The first one is using docker and the second one is running manual
### Start server with docker
- Go to the backend folder
- Run this command: docker-compose up
### Start server manually
- Go to the backend folder
- Run this command: pip install -r requirements.txt
- Run this command: python app.py
### Test API
- The server maybe take a moment to start (around 1-1.5 minutes). You should wait this process until you see the result as the below picture:
![be](https://github.com/nguyen-huu-luong/DEG4/assets/80337518/d9b64d67-de8c-4944-b11e-618ac9638339)
- After that, you can test this GET request. This process also takes a moment (about 1 minute for the first time): http://localhost:8000/test 
