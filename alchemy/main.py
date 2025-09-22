from web3 import Web3

# Connect to the Ethereum network
w3 = Web3(Web3.HTTPProvider('https://eth-mainnet.g.alchemy.com/v2/cC_WI1ML5bl2b5OehU5Kn'))

# Get block by number
block_number = "latest"  # Replace with the desired block number or use 'latest'
block = w3.eth.get_block(block_number)

print(block)