import java.io.File
import java.net.URLEncoder

import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.text.PDFTextStripper
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scalaj.http.Http

import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object App extends SparkEnvironnement {

  def main(args: Array[String]) {
    val filename = args(0)

    //########################
    // 1 Produce data
    val pdf = PDDocument.load(new File(filename))
    val stripper = new PDFTextStripper
    stripper.setStartPage(1)
    stripper.setEndPage(2)

    val input = stripper.getText(pdf).replace("-\n", "").replace(",\n", ",")

    var listEntities = new ListBuffer[String]()

    val keywordPattern: Regex = "Keywords\\n([^\\n]*)".r
    for (patternMatch <- keywordPattern.findAllMatchIn(input)) {
      //println(s"keyword found: ${patternMatch.group(0)}")
      //println(s"keys: ${patternMatch.group(1).split(",").map(_.trim).mkString("|")}")
      listEntities ++= patternMatch.group(1).split(",").map(_.trim)
    }

    //########################
    // 2 Create RDD to parallelize the processus
    val rddEntities = sc.parallelize(listEntities)

    //########################
    // 3 Search id and iri of wikidata
    // to use this method only if you have few data
    val rddEntitiesWithWikidata = rddEntities.map(entity =>
      {
        try {
          implicit val formats = DefaultFormats
          val response = Http("https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json&search=" + URLEncoder.encode(entity, "UTF-8") + "&language=en&type=item&limit=1").asString.body
          //println(response)
          // {"searchinfo":{"search":"test"},"search":[{"repository":"","id":"Q4115189","concepturi":"http://www.wikidata.org/entity/Q4115189","title":"Q4115189","pageid":4246474,"url":"//www.wikidata.org/wiki/Q4115189","label":"Wikidata Sandbox","description":"This is a sandbox for testing changes to items. Please be gentle with it. Feel free to change anything on this page! For testing links, try adding ones to userpages.","match":{"type":"alias","language":"en","text":"test"},"aliases":["test"]}],"search-continue":1,"success":1}
          val binResponse = parse(response)
          (entity, Some((binResponse \\ "label").extract[String]), Some((binResponse \\ "id").extract[String]), Some((binResponse \\ "concepturi").extract[String]))
        } catch {
          case _ => Tuple4[String, Option[String], Option[String], Option[String]](entity, None, None, None)
        }
      }
    )
    println(rddEntitiesWithWikidata.collect().mkString("\n"))
    //    (Linked Data,Some(linked data),Some(Q515701),Some(http://www.wikidata.org/entity/Q515701))
    //    (Open Science,Some(open science),Some(Q309823),Some(http://www.wikidata.org/entity/Q309823))
    //    (Wikidata,Some(Wikidata),Some(Q2013),Some(http://www.wikidata.org/entity/Q2013))
    //    (Wikipedia,Some(Wikipedia),Some(Q52),Some(http://www.wikidata.org/entity/Q52))
    //    (RDF,Some(Resource Description Framework),Some(Q54872),Some(http://www.wikidata.org/entity/Q54872))
    //    (SPARQL,Some(SPARQL),Some(Q54871),Some(http://www.wikidata.org/entity/Q54871))

    //########################
    // 4 Create RDF triples
    val subject = "https://hal.archives-ouvertes.fr/hal-01168496"
    val property = "http://www.wikidata.org/prop/direct/P921" //main topics
    val triples = rddEntitiesWithWikidata.flatMap(data => {
      if (data._4.isEmpty) {
        Nil
      } else {
        List(Tuple3(subject, property, data._4.get))
      }
    })
    println(triples.collect().mkString("\n"))
    //    (Linked Data,Some(linked data),Some(Q515701),Some(http://www.wikidata.org/entity/Q515701))
    //    (Open Science,Some(open science),Some(Q309823),Some(http://www.wikidata.org/entity/Q309823))
    //    (Wikidata,Some(Wikidata),Some(Q2013),Some(http://www.wikidata.org/entity/Q2013))
    //    (Wikipedia,Some(Wikipedia),Some(Q52),Some(http://www.wikidata.org/entity/Q52))
    //    (RDF,Some(Resource Description Framework),Some(Q54872),Some(http://www.wikidata.org/entity/Q54872))
    //    (SPARQL,Some(SPARQL),Some(Q54871),Some(http://www.wikidata.org/entity/Q54871))

    //########################
    // 5 Save triples in a file
    val p = new java.io.PrintWriter(filename + ".ttl")
    try {
      triples.collect().foreach(triple => p.println("<" + triple._1 + "> <" + triple._2 + "> <" + triple._3 + "> ."))
    } finally {
      p.close()
    }
    //    <https://hal.archives-ouvertes.fr/hal-01168496> <http://www.wikidata.org/prop/direct/P921> <http://www.wikidata.org/entity/Q515701> .
    //    <https://hal.archives-ouvertes.fr/hal-01168496> <http://www.wikidata.org/prop/direct/P921> <http://www.wikidata.org/entity/Q309823> .
    //    <https://hal.archives-ouvertes.fr/hal-01168496> <http://www.wikidata.org/prop/direct/P921> <http://www.wikidata.org/entity/Q2013> .
    //    <https://hal.archives-ouvertes.fr/hal-01168496> <http://www.wikidata.org/prop/direct/P921> <http://www.wikidata.org/entity/Q52> .
    //    <https://hal.archives-ouvertes.fr/hal-01168496> <http://www.wikidata.org/prop/direct/P921> <http://www.wikidata.org/entity/Q54872> .
    //    <https://hal.archives-ouvertes.fr/hal-01168496> <http://www.wikidata.org/prop/direct/P921> <http://www.wikidata.org/entity/Q54871> .
  }
}
