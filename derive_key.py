import os
import getpass
from eth_account import Account

def main():
    print("="*60)
    print("🔐 SCRIPT DE DERIVACIÓN SEGURA DE CLAVE PRIVADA")
    print("="*60)
    print("Este script derivará tu clave privada localmente y no la enviará a ningún lado.")
    print("Una vez inyectada en el archivo .env, este script se auto-destruirá.")
    print("-" * 60)
    
    # Enable HD wallet features
    Account.enable_unaudited_hdwallet_features()
    
    # Securely prompt for seed phrase
    seed_phrase = getpass.getpass("Ingresa tu frase semilla (12 o 24 palabras) [Oculto]: ")
    
    if not seed_phrase:
        print("❌ Error: No se ingresó ninguna frase semilla.")
        return
        
    try:
        # Derive the account using standard Ethereum path
        account = Account.from_mnemonic(seed_phrase.strip())
        private_key = account.key.hex()
        address = account.address
        
        # Read existing .env if it exists
        env_path = ".env"
        env_content = []
        key_found = False
        address_found = False
        
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                env_content = f.readlines()
                
        # Update or append PRIVATE_KEY and WALLET_ADDRESS
        for i, line in enumerate(env_content):
            if line.startswith("PRIVATE_KEY="):
                env_content[i] = f"PRIVATE_KEY={private_key}\n"
                key_found = True
            elif line.startswith("WALLET_ADDRESS="):
                env_content[i] = f"WALLET_ADDRESS={address}\n"
                address_found = True
                
        if not key_found:
            env_content.append(f"\nPRIVATE_KEY={private_key}\n")
        if not address_found:
            env_content.append(f"WALLET_ADDRESS={address}\n")
            
        # Write back to .env
        with open(env_path, 'w') as f:
            f.writelines(env_content)
            
        print(f"✅ Clave privada derivada exitosamente para la address: {address}")
        print(f"✅ Clave inyectada en {env_path} de forma segura.")
        
    except Exception as e:
        print(f"❌ Error al derivar la cuenta: {str(e)}")
    
    finally:
        # Auto-destruct
        try:
            script_path = os.path.abspath(__file__)
            os.remove(script_path)
            print(f"✅ Script auto-eliminado ({script_path}).")
        except Exception as e:
            print(f"⚠️ No se pudo auto-eliminar el script: {e}. Por favor, bórralo manualmente.")

if __name__ == "__main__":
    main()
