package storm.realTraffic.spout;


import java.io.IOException; 
//import org.dom4j.DocumentException; 
import org.w3c.dom.*; 
import org.xml.sax.*; 
import javax.xml.parsers.*; 

public class XMLReader { 
	public static void main(String arge[]) throws DOMException { 
		// 实例化一个文档构建器工厂 
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance(); 
		try { 
			// 通过文档构建器工厂获取一个文档构建器 
			DocumentBuilder db = dbf.newDocumentBuilder(); 
			// 通过文档通过文档构建器构建一个文档实例 
			Document doc = db.parse("tupleinfo.xml"); 
			// 获取所有名字为 “TURNOS” 的节点 
			NodeList nl1 = doc.getElementsByTagName("TURNOS"); 
			int size1 = nl1.getLength(); 
			for (int i = 0; i < size1; i++) { 
				Node n = nl1.item(i); 
				// 获取 n 节点下所有的子节点。此处值得注意，在DOM解析时会将所有回车都视为 n 节点的子节点。 
				NodeList nl2 = n.getChildNodes(); 
				// 因为上面的原因，在此例中第一个 n 节点有 2 个子节点，而第二个 n 节点则有 5 个子节点（因为多了3个回车）。 
				int size2 = nl2.getLength(); 
				for (int j = 0; j < size2; j++) { 
					Node n2 = nl2.item(j); 
					// 还是因为上面的原因，故此要处判断当 n2 节点有子节点的时才输出。 
					if (n2.hasChildNodes()) { 
						System.out.println(n2.getNodeName() + " = " 
								+ n2.getFirstChild().getNodeValue()); 
					} 
				} 
			} 
		} catch (ParserConfigurationException ex) { 
			ex.printStackTrace(); 
		} catch (IOException ex) { 
			ex.printStackTrace(); 
		} catch (SAXException ex) { 
			ex.printStackTrace(); 
		} 

	} 
} 