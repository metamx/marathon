package mesosphere.mesos

import mesosphere.marathon.core.instance.Instance
import mesosphere.marathon.core.task.Task.Reservation
import mesosphere.marathon.stream._
import org.apache.mesos.Protos.Offer
import org.apache.mesos.Protos.Resource
import org.apache.mesos.{Protos => Mesos}
import org.slf4j.LoggerFactory
import scala.collection.immutable.Seq

object PersistentVolumeMatcher {

  private[this] val log = LoggerFactory.getLogger(getClass)
  def matchVolumes(
    offer: Mesos.Offer,
    waitingInstances: Seq[Instance]): Option[VolumeMatch] = {

    // find all offered persistent volumes

    val availableVolumes: Map[String, Resource] = offer.getResourcesList.collect {
      case resource: Resource if resource.hasDisk && resource.getDisk.hasPersistence =>
        resource.getDisk.getPersistence.getId -> resource
    }(collection.breakOut)

    logOfferPersistentVolumeStatus(offer, availableVolumes)

    def resourcesForReservation(reservation: Reservation): Option[Seq[Resource]] = {
      if (reservation.volumeIds.map(_.idString).forall(availableVolumes.contains))
        Some(reservation.volumeIds.flatMap(id => availableVolumes.get(id.idString)))
      else
        None
    }

    waitingInstances.toStream
      .flatMap { instance =>
        // Note this only supports AppDefinition instances with exactly one task
        instance.tasksMap.values.headOption.flatMap(_.reservationWithVolumes).flatMap { reservation =>
          resourcesForReservation(reservation).flatMap(rs => Some(VolumeMatch(instance, rs)))
        }
      }.headOption
  }

  private def logOfferPersistentVolumeStatus(
    offer: Offer,
    availableVolumes: Map[String, Resource]
  ) = {
    if (log.isDebugEnabled) {
      val withDisk = offer.getResourcesList.count(r => r.hasDisk)
      val withPersistence = offer.getResourcesList.count(r => r.hasDisk && r.getDisk.hasPersistence)
      val pids = availableVolumes.keys.mkString(", ")
      log.debug(s"withDisk $withDisk, withPersist $withPersistence, pids [$pids]")
    }
  }

  case class VolumeMatch(instance: Instance, persistentVolumeResources: Seq[Mesos.Resource])
}
