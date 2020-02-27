## Info  
Cryptostore + Redis are setup on an EC2 instance, and are feeding data into our S3 bucket.   

#### Config Info
Cryptostore is configured to collect the following data:  
```
exchanges:
    BITMEX:
        retries: -1
        l2_book:
            symbols: [XBTUSD, ETHUSD]
            book_delta: true
        trades: [XBTUSD, ETHUSD]
        ticker: [XBTUSD, ETHUSD]
        funding: [XBTUSD, ETHUSD]
        open_interest: [XBTUSD, ETHUSD]
    COINBASE:
        retries: -1
        l3_book:
            symbols: [BTC-USD, ETH-USD, ETH-BTC]
            book_delta: true
        trades: [BTC-USD, ETH-USD, ETH-BTC]
        ticker: [BTC-USD, ETH-USD, ETH-BTC]
```    
Additionally, cryptostore is backfilling XBTUSD and BTC-USD trade data through Jan 1st 2017.   
#### Data Access
Data can be easily accessed and manipulated by mounting the S3 bucket on any EC2 instance.     
To accomplish this, most of [this](https://medium.com/tensult/aws-how-to-mount-s3-bucket-using-iam-role-on-ec2-linux-instance-ad2afd4513ef) tutorial should be followed.  

Notes:   
* On step 4 the tutorial has an error in the ./configure code. Please run the following instead:  
`./configure -prefix=/usr -with-openssl`
* On step 6, the IAM role has already been created, so skip to the attaching step between step 6 & 7.   
* On step 7, don't use the path they reccomend, just create a local dir called data as the mount point.  
* For step 8, use the following code instead:   
`s3fs quantfolio-ai-data-collection data`
