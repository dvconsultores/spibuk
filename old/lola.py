import requests

def Api_TMV():
    # URL de la API
    url = "https://v6.exchangerate-api.com/v6/51b85486ec98af8d07511e1c/latest/EUR"

    # Realizar la solicitud GET
    response = requests.get(url)

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        # Convertir la respuesta a JSON
        data = response.json()
        
        # Obtener el valor de 'conversion_rates' para 'VES'
        ves_value = data['conversion_rates'].get('VES')
        
        # Verificar si se encontró el valor
        if ves_value is not None:
            print(f"El valor de la conversión de EUR a VES es: {ves_value}")
            return ves_value  # También puedes devolver el valor si lo necesitas
        else:
            print("El valor de VES no se encontró en la respuesta.")
            return None
    else:
        print("Error en la solicitud:", response.status_code)
        return None  # Retornar None en caso de error

# Llamar a la función sin argumentos
Api_TMV()