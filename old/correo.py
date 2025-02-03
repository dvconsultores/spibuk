import smtplib
from email.mime.text import MIMEText

# Configura la conexión al servidor SMTP
server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login('jcuauro@gmail.com', 'mesf cfal sfwo brkr')

# Crea el mensaje de correo electrónico
msg = MIMEText('Este es el cuerpo del mensaje')
msg['Subject'] = 'Asunto del correo electrónico'
msg['From'] = 'tu_correo_electronico@example.com'
msg['To'] = 'jcuauro@gmail.com'

# Envía el correo electrónico
server.sendmail('jcuauro@gmail.com', 'jcuauro@gmail.com', msg.as_string())

# Cierra la conexión al servidor SMTP
server.quit()
