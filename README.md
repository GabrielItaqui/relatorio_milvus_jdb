# Relatório Milvus Headless

Este projeto é um script Python automatizado para geração de relatórios diários de atendimento utilizando a API Milvus. Ele realiza o processamento dos dados, envia e-mails com relatórios anexados, alerta técnicos via WhatsApp e atualiza planilhas mensais de horas trabalhadas.

## Funcionalidades

- **Coleta automática de dados** da API Milvus.
- **Processamento e limpeza** dos dados recebidos em CSV.
- **Geração de relatórios** diários em formato CSV.
- **Envio automático de e-mails** com o relatório em anexo para os destinatários configurados.
- **Alerta via WhatsApp** para técnicos com horas abaixo do mínimo esperado.
- **Atualização automática de planilhas Excel** mensais com o resumo de horas por técnico.
- **Envio do log de execução** por e-mail e limpeza automática de arquivos temporários.

## Requisitos

- Python 3.8+
- Conta de e-mail SMTP para envio automático
- API Token Milvus
- Conta WhatsApp Web ativa (PyWhatKit)
- Planilhas mensais no formato esperado (Excel)

### Principais dependências
- pandas
- requests
- openpyxl
- pywhatkit
- python-dotenv

Instale todas as dependências com:

```bash
pip install -r requirements.txt
```

## Configuração

1. **Variáveis de ambiente**: Crie um arquivo `.env` na raiz do projeto com as seguintes variáveis:

```
MILVUS_API_TOKEN=seu_token_api
SMTP_SERVER=smtp.seuprovedor.com
SMTP_PORT=587
EMAIL_REMETENTE=seu@email.com
SENHA_REMETENTE=sua_senha
DESTINATARIOS_EMAIL=dest1@email.com,dest2@email.com
EMAIL_DESTINATARIO_LOG=log@email.com
TECNICOS_A_IGNORAR_LIST=Nome1,Nome2
WHATSAPP_TECNICOS_JSON={"tecnico1":"+5511999999999"}
```

2. **Planilhas Excel**: Certifique-se de que as planilhas mensais estejam no diretório configurado em `BASE_PASTA_RELATORIOS` e sigam o padrão de cabeçalho esperado pelo script.

3. **WhatsApp Web**: O envio de mensagens via PyWhatKit requer que o WhatsApp Web esteja autenticado no navegador padrão.

## Execução

Execute o script principal:

```bash
python relatorio_milvus_headless.py
```

O script irá:
- Buscar os dados do último dia útil
- Processar e salvar o relatório CSV
- Enviar o e-mail com o relatório
- Enviar alertas via WhatsApp (se necessário)
- Atualizar a planilha mensal
- Enviar o log de execução por e-mail

## Observações
- O script é robusto contra falhas de conexão e erros de API.
- Todos os logs são salvos e enviados por e-mail ao final da execução.
- Arquivos temporários são removidos automaticamente.

## Licença

Este projeto é privado e para uso interno da equipe JDB Tecnologia.

---

Dúvidas ou sugestões? Entre em contato com o responsável pelo script.
