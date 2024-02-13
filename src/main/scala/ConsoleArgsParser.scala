import scala.annotation.tailrec
import scala.sys.exit

object ConsoleArgsParser {
	
	@tailrec
	def getArgs(map: Map[String, String], list: List[String]): Map[String, String] = {
		list match {
			case Nil => map
			case "--targetDirPath" :: value :: tail =>
				getArgs(map ++ Map("targetDirPath" -> value), tail)
			case "--sourceDirPath" :: value :: tail =>
				getArgs(map ++ Map("sourceDirPath" -> value), tail)
			case unknown :: _ =>
				println("Unknown option " + unknown)
				exit(1)
		}
	}
}
