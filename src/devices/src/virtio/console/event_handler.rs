use std::os::unix::io::AsRawFd;

use polly::event_manager::{EventManager, Subscriber};
use utils::epoll::{EpollEvent, EventSet};

use super::device::Console;
use crate::virtio::console::device::{CONTROL_RXQ_INDEX, CONTROL_TXQ_INDEX};
use crate::virtio::console::port_queue_mapping::{queue_idx_to_port_id, QueueDirection};
use crate::virtio::device::VirtioDevice;

impl Console {
    pub(crate) fn read_queue_event(&self, queue_index: usize, event: &EpollEvent) -> bool {
        log::trace!("Event on queue {queue_index}: {:?}", event.event_set());

        let event_set = event.event_set();
        if event_set != EventSet::IN {
            warn!("Unexpected event from queue index {queue_index}: {event_set:?}");
            return false;
        }

        if let Err(e) = self.queue_events[queue_index].read() {
            error!("Failed to read event from queue index {queue_index}: {e:?}");
            return false;
        }

        true
    }

    fn notify_port_queue_event(&mut self, queue_index: usize) {
        let (direction, port_id) = queue_idx_to_port_id(queue_index);
        match direction {
            QueueDirection::Rx => {
                log::trace!("Notify rx (queue event)");
                self.ports[port_id].notify_rx()
            }
            QueueDirection::Tx => {
                log::trace!("Notify tx (queue event)");
                self.ports[port_id].notify_tx()
            }
        }
    }

    fn handle_activate_event(&self, event_manager: &mut EventManager) {
        debug!("console: activate event");
        if let Err(e) = self.activate_evt.read() {
            error!("Failed to consume console activate event: {e:?}");
        }

        // The subscriber must exist as we previously registered activate_evt via
        // `interest_list()`.
        let self_subscriber = event_manager
            .subscriber(self.activate_evt.as_raw_fd())
            .unwrap();

        for queue_index in 0..self.queues.len() {
            event_manager
                .register(
                    self.queue_events[queue_index].as_raw_fd(),
                    EpollEvent::new(
                        EventSet::IN,
                        self.queue_events[queue_index].as_raw_fd() as u64,
                    ),
                    self_subscriber.clone(),
                )
                .unwrap_or_else(|e| {
                    error!(
                        "Failed to register queue index {queue_index} with event manager: {e:?}"
                    );
                });
        }

        event_manager
            .unregister(self.activate_evt.as_raw_fd())
            .unwrap_or_else(|e| {
                error!("Failed to unregister fs activate evt: {e:?}");
            })
    }

    fn handle_sigwinch_event(&mut self, event: &EpollEvent) {
        debug!("console: SIGWINCH event");

        let event_set = event.event_set();
        if event_set != EventSet::IN {
            warn!("console: sigwinch unexpected event {event_set:?}");
        }

        if let Err(e) = self.sigwinch_evt.read() {
            error!("Failed to read the sigwinch event: {e:?}");
        }

        for i in 0..self.ports.len() {
            if let Some(term) = self.ports[i].terminal() {
                let (cols, rows) = term.get_win_size();
                self.update_console_size(i as u32, cols, rows);
            }
        }
    }

    fn read_control_queue_event(&mut self, event: &EpollEvent) {
        let event_set = event.event_set();
        if event_set != EventSet::IN {
            warn!("Unexpected event {event_set:?}");
        }

        if let Err(e) = self.control.queue_evt().read() {
            error!("Failed to read the ConsoleControl event: {e:?}");
        }
    }
}

impl Subscriber for Console {
    fn process(&mut self, event: &EpollEvent, event_manager: &mut EventManager) {
        let source = event.fd();

        // `interest_list()` (below) registers `activate_evt`, `sigwinch_evt`,
        // and `control.queue_evt()` with epoll the moment the Console
        // Subscriber is constructed — well before `activate()` populates
        // `queue_events`. If any of those eventfds fire pre-activation, we
        // enter this function with `queue_events` still empty
        // (`Vec::new()`), so indexing `queue_events[CONTROL_RXQ_INDEX]` (2)
        // or `[CONTROL_TXQ_INDEX]` (3) panics with
        // `index out of bounds: the len is 0 but the index is 2`.
        // The `else` arm at the bottom of this function explicitly handles
        // the spurious-pre-activation case via `warn!()`, but is unreachable
        // while the indexed reads happen unconditionally up top. Defer them
        // into the activated branch so we actually hit the warn path.
        let control_rxq_control = self.control.queue_evt().as_raw_fd();

        let activate_evt = self.activate_evt.as_raw_fd();
        let sigwinch_evt = self.sigwinch_evt.as_raw_fd();

        if self.is_activated() {
            let control_rxq = self.queue_events[CONTROL_RXQ_INDEX].as_raw_fd();
            let control_txq = self.queue_events[CONTROL_TXQ_INDEX].as_raw_fd();

            let mut raise_irq = false;

            if source == control_txq {
                raise_irq |=
                    self.read_queue_event(CONTROL_TXQ_INDEX, event) && self.process_control_tx()
            } else if source == control_rxq_control {
                self.read_control_queue_event(event);
                raise_irq |= self.process_control_rx();
            } else if source == control_rxq {
                raise_irq |= self.read_queue_event(CONTROL_RXQ_INDEX, event)
            }
            /* Guest signaled input/output on port */
            else if let Some(queue_index) = self
                .queue_events
                .iter()
                .position(|fd| fd.as_raw_fd() == source)
            {
                raise_irq |= self.read_queue_event(queue_index, event);
                self.notify_port_queue_event(queue_index);
            } else if source == activate_evt {
                self.handle_activate_event(event_manager);
            } else if source == sigwinch_evt {
                self.handle_sigwinch_event(event);
            } else {
                log::warn!("Unexpected console event received: {source:?}")
            }
            if raise_irq {
                self.device_state.signal_used_queue();
            }
        } else {
            warn!("console: The device is not yet activated. Spurious event received: {source:?}");
            // Drain whichever pre-activation eventfd fired so epoll
            // doesn't keep redelivering the same edge in a tight loop.
            // The Subscriber is registered with three fds via
            // `interest_list()`: activate_evt, sigwinch_evt, and
            // control.queue_evt(). The first never legitimately fires
            // pre-activation (the device's `activate()` writes to it
            // *after* flipping `device_state` to Activated), so any
            // pre-activation hit on it is genuinely spurious. The
            // other two can fire pre-activation if the guest pokes
            // virtio-console control / SIGWINCH paths during early
            // probing; we just drain them so epoll quiesces.
            if source == self.control.queue_evt().as_raw_fd() {
                let _ = self.control.queue_evt().read();
            } else if source == self.sigwinch_evt.as_raw_fd() {
                let _ = self.sigwinch_evt.read();
            } else if source == self.activate_evt.as_raw_fd() {
                let _ = self.activate_evt.read();
            }
        }
    }

    fn interest_list(&self) -> Vec<EpollEvent> {
        vec![
            EpollEvent::new(EventSet::IN, self.activate_evt.as_raw_fd() as u64),
            EpollEvent::new(EventSet::IN, self.sigwinch_evt.as_raw_fd() as u64),
            EpollEvent::new(EventSet::IN, self.control.queue_evt().as_raw_fd() as u64),
        ]
    }
}
