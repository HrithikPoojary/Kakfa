import json

word  = '''{
    "InvoiceNumber": "51402977",
    "CreatedTime": 1595688900348,
    "StoreID": "STR7188",
    "PosID": "POS956",
    "CashierID": "OAS134",
    "CustomerType": "PRIME",
    "CustomerCardNo": "4629185211",
    "TotalAmount": 11114.0,
    "NumberOfItems": 4,
    "PaymentMethod": "CARD",
    "TaxableAmount": 11114.0,
    "CGST": 277.85,
    "SGST": 277.85,
    "CESS": 13.8925,
    "DeliveryType": "TAKEAWAY",
    "InvoiceLineItems": [
        {
            "ItemCode": "458",
            "ItemDescription": "Wine glass",
            "ItemPrice": 1644.0,
            "ItemQty": 2,
            "TotalValue": 3288.0
        },
        {
            "ItemCode": "283",
            "ItemDescription": "Portable Lamps",
            "ItemPrice": 2236.0,
            "ItemQty": 1,
            "TotalValue": 2236.0
        },
        {
            "ItemCode": "498",
            "ItemDescription": "Carving knifes",
            "ItemPrice": 1424.0,
            "ItemQty": 2,
            "TotalValue": 2848.0
        },
        {
            "ItemCode": "523",
            "ItemDescription": "Oil-lamp clock",
            "ItemPrice": 1371.0,
            "ItemQty": 2,
            "TotalValue": 2742.0
        }
    ]
}'''

invoicedata = json.loads(word)
print(invoicedata['InvoiceLineItems'][0]['ItemCode'])



