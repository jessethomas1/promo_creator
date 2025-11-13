select 

product2.PN_Article_Id__c as article_id, 
su.id,
product2.id as pid_id

from picnic_nl_prod.meltano_salesforce.product2 as product2 
inner join picnic_nl_prod.meltano_salesforce.Selling_Unit__c as su
on product2.id = su.ar_picnic_article__c
where ar_article_quantity__c = 1