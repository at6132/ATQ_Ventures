from mexc_python.mexcpy.api import MexcFuturesAPI as API
import asyncio
import getpass

USER_TOKEN = "WEB029506898cc35436d28428665685e59061ae4b900a6f95e06ab25a0341f438f1"  # Replace or prompt below

async def main():
    token = USER_TOKEN
    if token == "YOUR_USERTOKEN_HERE":
        token = getpass.getpass('Enter your MEXC USER_TOKEN: ')
    mexc_api = API(token=token)
    resp = await mexc_api.get_user_asset('USDT')
    if resp.success and hasattr(resp.data, 'availableBalance'):
        print(f"USDT available balance: {resp.data.availableBalance}")
    else:
        print(f"Failed to fetch balance: {resp}")

if __name__ == "__main__":
    asyncio.run(main()) 