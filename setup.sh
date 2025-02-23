#!/bin/bash
# Environment Preparation Script

VENV_PATH="./venv"

if [ -d "$VENV_PATH" ]; then
  echo "Ativando o ambiente virtual..."
  source "$VENV_PATH/bin/activate"
else
  echo "Ambiente virtual não encontrado em $VENV_PATH"
  echo "Crie um ambiente virtual com: python3 -m venv $VENV_PATH"
  exit 1
fi

# Python Packages
if [ -f "requirements.txt" ]; then
  echo "Instalando os pacotes Python..."
  pip install -r requirements.txt
else
  echo "Arquivo requirements.txt não encontrado."
fi