# 一旦获取数据，就直接存入数据库，如果数据库存在数据，则更新或者不采取操作，此程序每3天运行一次即可
from neo4j.v1 import GraphDatabase
from loguru import logger
uri = "bolt://localhost:7687"
_driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))

class Neo4jRunner(object):

    def __init__(self):
        uri = "bolt://localhost:7687"
        _driver = GraphDatabase.driver(uri, auth=("neo4j", "neo4j"))
        self._driver =_driver

    def close(self):
        self._driver.close()

    @classmethod
    def creat_company(self,tx, company):
        try:
            tx.run(
                "MERGE (p:ncompany{cname:$cname,cename:$cename,cuname:$cuname,acode:$acode,acname:$acname,bcode:$bcode,bcname:$bcname,hcode:$hcode,hcname:$hcname,stType:$stType,ecotype:$ecotype,exmarket:$exmarket,bexmarket:$bexmarket,aldoshi:$aldoshi,phone:$phone,email:$email,tax:$tax,website:$website,warea:$warea,sarea:$sarea,area:$area,postcode:$postcode,signcoin:$signcoin,buscode:$buscode,hnum:$hnum,mnum:$mnum,lesta:$lesta,kusta:$kusta,intro:$intro,buarea:$buarea,sdate:$sdate,bdate:$bdate,fabei:$fabei,onlinedate:$onlinedate,faway:$faway,evalue:$evalue,fasize:$fasize,favalue:$favalue,facost:$facost,fa_avalue:$fa_avalue,collcoin:$collcoin,firmon:$firmon,firback:$firback,fircha:$fircha,firsthigh:$firsthigh,onliqian:$onliqian,inqian:$inqian})",
                cname=company['jbzl']['gsmc'], cename=company['jbzl']['ywmc'], cuname=company['jbzl']['cym'],
                acode=company['jbzl']['agdm'], acname=company['jbzl']['agjc'], bcode=company['jbzl']['bgdm'],
                bcname=company['jbzl']['bgjc'], hcode=company['jbzl']['hgdm'], hcname=company['jbzl']['hgjc'],
                stType=company['jbzl']['zqlb'], ecotype=company['jbzl']['sshy'], exmarket=company['jbzl']['ssjys'],
                bexmarket=company['jbzl']['sszjhhy'], aldoshi=company['jbzl']['dlds'], phone=company['jbzl']['lxdh'],
                email=company['jbzl']['dzxx'], tax=company['jbzl']['cz'], website=company['jbzl']['gswz'],
                warea=company['jbzl']['bgdz'], sarea=company['jbzl']['zcdz'], area=company['jbzl']['qy'],
                postcode=company['jbzl']['yzbm'], signcoin=company['jbzl']['zczb'], buscode=company['jbzl']['gsdj'],
                hnum=company['jbzl']['gyrs'], mnum=company['jbzl']['glryrs'], lesta=company['jbzl']['lssws'],
                kusta=company['jbzl']['kjssws'], intro=company['jbzl']['gsjj'], buarea=company['jbzl']['jyfw'],
                sdate=company['fxxg']['clrq'], bdate=company['fxxg']['ssrq'], fabei=company['fxxg']['fxsyl'],
                onlinedate=company['fxxg']['wsfxrq'], faway=company['fxxg']['fxfs'], evalue=company['fxxg']['mgmz'],
                fasize=company['fxxg']['fxl'], favalue=company['fxxg']['mgfxj'], facost=company['fxxg']['fxfy'],
                fa_avalue=company['fxxg']['fxzsz'], collcoin=company['fxxg']['mjzjje'], firmon=company['fxxg']['srkpj'],
                firback=company['fxxg']['srspj'], fircha=company['fxxg']['srhsl'], firsthigh=company['fxxg']['srzgj'],
                onliqian=company['fxxg']['wxpszql'], inqian=company['fxxg']['djzql'])
        except:
            logger.info(f"creat company node {company['jbzl']['gsmc']} error!")

    @classmethod
    def creat_person(self,tx, person):
        try:
            tx.run("MERGE (p:Person {name:$name})", name=person)
        except:
            logger.info(f"creat company node {person} error!")

    @classmethod
    # 公司和总经理的关系
    def creat_coma(self,tx, person, cname):
        try:
            tx.run("MATCH (FROM:ncompany{cname:$cname}), (To:Person{name:$name})  "
                   "MERGE (FROM)-[r:coma{cname:$cname,name:$name}]-> (To)",
                   name=person, cname=cname)
        except:
            logger.info(f"creat coma error!")

    @classmethod
    # 公司和法人代表的关系
    def creat_core(self,tx, person, cname):
        try:
            tx.run("MATCH (FROM:ncompany{cname:$cname}), (To:Person{name:$name})  "
                   "MERGE (FROM)-[r:core{cname:$cname,name:$name}]-> (To)",
                   name=person, cname=cname)
        except:
            logger.info(f"creat core error!")

    @classmethod
    # 建立公司和董事长关系
    def creat_codo(self,tx, person, cname):
        try:
            tx.run("MATCH (FROM:ncompany{cname:$cname}), (To:Person{name:$name})  "
                   "MERGE (FROM)-[r:codo{cname:$cname,name:$name}]-> (To)",
                   name=person, cname=cname)
        except:
            logger.info(f"creat codo error!")


    @classmethod
    def do_all(self,company):
        try:
            with _driver.session() as session_a:
                session_a.write_transaction(self.creat_company, company)
                session_a.write_transaction(self.creat_person, company['jbzl']['zjl'])
                session_a.write_transaction(self.creat_person, company['jbzl']['frdb'])
                session_a.write_transaction(self.creat_person, company['jbzl']['dsz'])
                session_a.write_transaction(self.creat_coma, company['jbzl']['zjl'], company['jbzl']['gsmc'])
                session_a.write_transaction(self.creat_core, company['jbzl']['frdb'], company['jbzl']['gsmc'])
                session_a.write_transaction(self.creat_codo, company['jbzl']['dsz'], company['jbzl']['gsmc'])
        except:
            logger.info(f"create all error!")
    # Create a person node.
    # @classmethod
    # def create_person(cls, tx, name):
    #     tx.run("CREATE (:Person {name: $name})", name=name)
    #
    # @classmethod
    # ##创建公司信息，里面需要检测人物是否存在
    # def create_company(cls,tx,company):
    #     pass
    # # Create an employment relationship to a pre-existing company node.
    # # This relies on the person first having been created.
    #
    # @classmethod
    # #创建公司关系和人关系
    # def create_belong(cls,cname,cperson):
    #     pass
    # # Create a friendship between two people.
    # @classmethod
    # def create_friendship(cls, tx, name_a, name_b):
    #     tx.run("MATCH (a:Person {name: $name_a}) "
    #            "MATCH (b:Person {name: $name_b}) "
    #            "MERGE (a)-[:KNOWS]->(b)",
    #            name_a=name_a, name_b=name_b)
    #
    # # Match and display all friendships.
    # @classmethod
    # def print_friendships(cls, tx):
    #     result = tx.run("MATCH (a)-[:KNOWS]->(b) RETURN a.name, b.name")
    #     for record in result:
    #         print("{} knows {}".format(record["a.name"] ,record["b.name"]))
    #
    # def main(self):
    #     saved_bookmarks = []  # To collect the session bookmarks
    #
    #     # Create the first person and employment relationship.
    #     with self._driver.session() as session_a:
    #         session_a.write_transaction(self.create_person, "Alice")
    #         session_a.write_transaction(self.employ, "Alice", "Wayne Enterprises")
    #         saved_bookmarks.append(session_a.last_bookmark())
    #
    #     # Create the second person and employment relationship.
    #     with self._driver.session() as session_b:
    #         session_b.write_transaction(self.create_person, "Bob")
    #         session_b.write_transaction(self.employ, "Bob", "LexCorp")
    #         saved_bookmarks.append(session_b.last_bookmark())
    #
    #     # Create a friendship between the two people created above.
    #     with self._driver.session(bookmarks=saved_bookmarks) as session_c:
    #         session_c.write_transaction(self.create_friendship, "Alice", "Bob")
    #         session_c.read_transaction(self.print_friendships)



