import scrapy
import pandas as pd
from bs4 import BeautifulSoup
import json
import re

class AntiqSpider(scrapy.Spider):
    name = "antiq"
    start_urls = [""]
    image_list = []
    handle_httpstatus_list = [301]

    def parse(self, response):
        for i in range(1, 25):
            # url = f"https://www.1stdibs.com/search/furniture/?page={2}&q=alexanders%20antiques"
            url = f"https://www.1stdibs.com/dealers/alexanders-antiques/shop/?page={i}"
            yield scrapy.Request(url, callback=self.data_link)
            
    def data_link(self,response):
        paths = response.xpath("//div[@id = 'js-root']//div[@class = '_95b421a2']")
        for path in paths:
            link = path.xpath(".//a/@href").get()
            if link is not None:
                absolute_url = f"https://www.1stdibs.com{link}"
                yield scrapy.Request(absolute_url,callback=self.parse_link)
            
    def parse_link(self, response):
        category = "" 
        category2 = ""
        finalcategory = ""
        dimension = str(0)
        Categories = response.xpath("//div[@class = '_ea16259b'][2]/nav/ol/li")
        
        for idx, cat in enumerate(Categories):
            cat_value = cat.xpath(".//a/text()").get()
            if cat_value and idx != 0:
               if idx == 1: 
                    category2 += f",{cat_value},"
               else:
                   category2 += f"{cat_value},"
                   
            if cat_value and idx != 0:
                if idx == 1:
                    category += f"{cat_value}" 
                else:
                    category += f" >{cat_value}" 
                    
            finalcategory = f"{category} {category2}"
            

        imagepath = response.xpath("//div[@class = '_e382a858']/div/div[1]")
        soup = BeautifulSoup(response.text, 'html.parser')
        script_tag = soup.find('script', {'type': 'application/ld+json'})
        json_data = json.loads(script_tag.string)
        price = str(0)
        description = str(0)
        name = str(0)

        for item in json_data:
            if item.get('@type') == 'Product':
                price = item.get('offers', {}).get('price')
                description = item.get('description')
                name = item.get('name')
                price = price
                description = description
                dimension_pattern = r"Dimensions\s*(.*)"

                match = re.search(dimension_pattern, description, re.DOTALL | re.IGNORECASE)

                if match:
                    dimension = match.group(1).strip()
                    
                    
                # dimension_pattern2 = r"(Condition\s*.*)"

                # match2 = re.search(dimension_pattern, dimension, re.DOTALL | re.IGNORECASE)

                # if match2:
                #     post_dimensions_content = match2.group(1).strip()
                #     dimension = dimension.replace(post_dimensions_content, '').strip()
                    
                    
                    
                    
                dimension_pattern2 = r"(Dimensions\s*.*)"

                match2 = re.search(dimension_pattern2, description, re.DOTALL | re.IGNORECASE)

                if match2:
                    post_dimensions_content = match2.group(1).strip()
                    description = description.replace(post_dimensions_content, '').strip()
                
                
       
        for image in imagepath:
            imageLink = image.xpath("//button[1]")
            for link in imageLink:
                datalink = link.xpath(".//img/@src").get()
                if datalink is not None:
                    self.image_list.append({
                        "Title":name,
                        "ImageLink":datalink,
                        "Price":price,
                        "Category":finalcategory,
                        "Description":description,
                        "Dimension": dimension if dimension else None,
                    })
            
    def close(self, reason):
        self.to_csv(self.image_list)

    def to_csv(self, image):
        df = pd.DataFrame(image)
        df = df.drop_duplicates(subset=['Title'])
        print("##################### print length ###########################################")
        df_grouped = df.groupby('Title').agg({
            'ImageLink': lambda x: ', '.join(x.unique()), 
            'Price': 'first',
            'Category':'first',
            'Description': 'first',
            'Dimension':'first',
        }).reset_index()

        df_grouped.to_csv("data2.csv", index=False)
