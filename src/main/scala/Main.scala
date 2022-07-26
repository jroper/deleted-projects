package kalix.scripts

import java.time.Instant
import java.time.format.DateTimeFormatter

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import play.api.libs.json.{JsObject, Json, Writes}

import scala.sys.process._
import scala.util.control.NonFatal
import scala.jdk.CollectionConverters._

object Main extends App {
  val context = "prod"

  private val mapper = new ObjectMapper
  private val jsonFactory = new JsonFactory(mapper)

  case class Project(id: String, friendlyName: String, created: Option[Instant], deleted: Option[Instant], owner: String, ownerFriendlyName: String)

  val deletedProjects =
    Json.parse(s"kalixadmin --context $context projects list --deleted -o json".!!)
      .as[Seq[JsObject]]
      .map { js =>
        val name = (js \ "name").as[String]
        val id = name.stripPrefix("projects/")
        val friendlyName = (js \ "friendly_name").as[String]
        val (owner, ownerFriendlyName) = if ((js \ "Owner" \ "UserOwner").isDefined) {
          ("users/" + (js \ "Owner" \ "UserOwner" \ "id").as[String], (js \ "Owner" \ "UserOwner" \ "friendly_name").as[String])
        } else {
          ("organizations/" + (js \ "Owner" \ "OrganizationOwner" \ "id").as[String], (js \ "Owner" \ "OrganizationOwner" \ "friendly_name").as[String])
        }

        name -> Project(id, friendlyName, None, None, owner, ownerFriendlyName)
      }

  val count = deletedProjects.size
  println(s"Retrieved $count deleted projects.")

  // This is to work around rate limiting
  def tryThenWait[T](block: => T): T = {
    try {
      block
    } catch {
      case NonFatal(_) =>
        println("Got error, probably hit the rate limit. Sleeping for one minute to let the quotas reset, then trying again...")
        Thread.sleep(65000)
        block
    }
  }

  val withTimestamps = deletedProjects.zipWithIndex.map {
    case ((name, project), idx) =>
      println(s"${idx + 1}/$count Fetching audit log for $name...")
      val json = tryThenWait(s"kalixadmin --context $context audit entity $name -o json".!!)
      // kalixadmin audit outputs json objects without putting them in a json array. Jacksons ObjectMapper.readValues
      // can handle this.
      val log = mapper.readValues(jsonFactory.createParser(json), classOf[JsonNode]).readAll().asScala.toSeq
        .map(Writes.jsonNodeWrites.writes)
      val deleted = log.collectFirst {
        case deleted if (deleted \ "Type").as[String] == "ProjectDeleted" =>
          (deleted \ "Timestamp").as[Instant]
      }
      val created = log.collectFirst {
        case created if (created \ "Type").as[String] == "ProjectCreated" =>
          (created \ "Timestamp").as[Instant]
      }
      project.copy(deleted = deleted, created = created)
  }

  println()
  println("Project id,Friendly name,Created,Deleted,Owner,Owner Friendly Name")
  withTimestamps.sortBy(_.deleted).foreach { p =>
    println(s"""${p.id},"${p.friendlyName}",${p.created.fold("")(DateTimeFormatter.ISO_INSTANT.format)},${p.deleted.fold("")(DateTimeFormatter.ISO_INSTANT.format)},${p.owner},"${p.ownerFriendlyName}"""")
  }

}
