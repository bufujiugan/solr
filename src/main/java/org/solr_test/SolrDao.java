package org.solr_test;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;  
import org.apache.solr.client.solrj.SolrServerException;  
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.HttpSolrServer;  
import org.apache.solr.client.solrj.response.QueryResponse;  
import org.apache.solr.common.SolrDocument;  
import org.apache.solr.common.SolrDocumentList;  
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;  
  
/** 
 * 操作Solr服务端的索引库 
 * <p>Title: SolrDao</p> 
 * <p>Description: </p> 
 * <p>Company:</p>  
 * @author  夏 杰 
 * @date    2016年1月25日下午3:44:43 
 * @version 1.0 
 */  
public class SolrDao {  
    /** 
     * 往索引库添加文档 
     * @throws IOException  
     * @throws SolrServerException  
     */  
    @Test
    public void addDoc() throws SolrServerException, IOException{  
        //这里可以从文件里读或数据库里读  
          
        //构造一篇文档  
        SolrInputDocument document = new SolrInputDocument();  
        //往doc中添加字段,在客户端这边添加的字段必须在服务端中有过定义  
        document.addField("id", "number001");  
        document.addField("title", "123465");  
        document.addField("content", "ONLY冬新长款系带含羊毛毛呢外套女");  
          
        //获得一个solr服务端的请求，去提交  
        HttpSolrServer solrClient = new HttpSolrServer("http://localhost:8983/solr/");  
        solrClient.add(document);
        solrClient.commit();  
    }
      
    /** 
     * 根据id删除文档 
     * 从索引库删除文档 
     */  
    @Test  
    public void deleteDocumentById() throws Exception {  
        HttpSolrServer server = new HttpSolrServer("http://localhost:8983/solr/");  
        //删除文档  
//        server.deleteById("number001");  
        //提交修改  
        server.commit();  
    }
          
    //根据查询删除文档  
    @Test  
    public void deleteDocumentByQuery() throws Exception {  
        HttpSolrServer server = new HttpSolrServer("http://localhost:8983/solr/");  
        //根据查询条件删除  
//        server.deleteByQuery("*:*");  
        //提交修改  
        server.commit();  
    }  
      
     /** 
       * 对索引库中的文档进行更新 
     * @throws SolrServerException
     */  
    @Test  
    public void query() throws SolrServerException{  
        HttpSolrServer server = new HttpSolrServer("http://localhost:8983/solr/");  
        SolrQuery query = new SolrQuery();  
        //给query设置一个主查询条件  
        //query.set("q", "*:*");//查询所有  
          
        query.set("q","台灯");  
          
        //给query增加过滤查询条件  
        query.addFilterQuery("product_price:[0 TO 200]");  
          
        //给query增加布尔过滤条件  
        //query.addFilterQuery("-product_name:台灯");  
          
        //给query设置默认搜索域  
        query.set("df", "product_keywords");  
          
        //设置返回结果的排序规则  
        query.setSort("product_price",ORDER.desc);  
          
        //设置分页参数  
        query.setStart(0);  
        query.setRows(20);//每一页多少值  
          
        //设置高亮  
        query.setHighlight(true);  
        //设置高亮的字段  
        query.addHighlightField("product_name");  
        //设置高亮的样式  
        query.setHighlightSimplePre("<em>");  
        query.setHighlightSimplePost("</em>");  
        QueryResponse response = server.query(query);  
        //查询得到文档的集合  
        SolrDocumentList solrDocumentList = response.getResults();  
        System.out.println("查询结果的总数量：" + solrDocumentList.getNumFound());  
          
        //遍历列表  
        for (SolrDocument solrDocument : solrDocumentList) {  
            System.out.println(solrDocument.get("id"));  
            System.out.println(solrDocument.get("product_name"));  
            System.out.println(solrDocument.get("product_price"));  
            System.out.println(solrDocument.get("product_catalog_name"));  
            System.out.println(solrDocument.get("product_picture"));  
        }  
    }
    
    
    // 查询
    @Test
    public void testQueryAll() {
    	SolrServer server = new HttpSolrServer("http://localhost:8983/solr/");
        SolrQuery params = new SolrQuery();

        // 查询关键词，*:*代表所有属性、所有值，即所有index
        params.set("q", "羊绒"); // 搜索的词修改它就进行查询了

        // 分页，start=0就是从0开始，rows=5当前返回5条记录，第二页就是变化start这个值为5就可以了。
        params.set("start", 0);
        params.set("rows", "5");

        // 排序，如果按照id排序，那么 写为： id desc(or asc)
//        params.set("sort", "id asc");

        QueryResponse response = null;
        try {
            response = server.query(params);
        } catch (SolrServerException e) {
            e.printStackTrace();
        }

        if (response != null) {
            System.out.println("Search Results: ");
            SolrDocumentList list = response.getResults();
            for (int i = 0; i < list.size(); i++) {
                System.out.println(list.get(i).get("title"));//根据key取值
            }
        }
    }
    
    /**
     * 新增个文档并且自己新建一个节点
     * @throws SolrServerException 
     */
    @Test
    public void addDocElement() throws SolrServerException {
    	// step1 创建一个SolrServer对象
    	SolrServer solrServer = new HttpSolrServer("http://localhost:8983/solr/");
    	// step2 创建一个SolrQuery对象
    	SolrQuery solrQuery = new SolrQuery();
    	// step3 向SolrQuery中添加过滤条件查询条件
    	solrQuery.setQuery("apple");
    	// step4 指定默认搜索域
//    	solrQuery.set("df", "itemKeywords");
    	// step5 开启高亮显示
    	solrQuery.setHighlight(true);
    	// step6 高亮显示的域
    	solrQuery.addHighlightField("itemTitle");
    	solrQuery.setHighlightSimplePre("<em>");
    	solrQuery.setHighlightSimplePre("</em>");
    	// step7  执行查询，得到一个Response对象
    	QueryResponse response = solrServer.query(solrQuery);
    	// step8 取出查询结果
    	SolrDocumentList results = response.getResults();
    	System.out.println("总数量: " + results.getNumFound());
    	// step9 循环遍历取高亮值
    	for (SolrDocument result : results) {
			System.out.println(result.get("id"));
			Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();
			List<String> list = highlighting.get(result.get("id")).get("itemTitle");
			String itemTitle = null;
			if(null != list && list.size() > 0) {
				itemTitle = list.get(0);
			} else {
				itemTitle = (String) result.get("itemTitle");
			}
			System.out.println(itemTitle);
			System.out.println(result.get("itemTitle"));
		}
    }
    
}  