package Travelynx::Command::work;
use Mojo::Base 'Mojolicious::Command';

use DateTime;
use JSON;
use List::Util;

has description =>
  'Perform automatic checkout when users arrive at their destination';

has usage => sub { shift->extract_usage };

sub run {
	my ($self) = @_;

	my $now  = DateTime->now( time_zone => 'Europe/Berlin' );
	my $json = JSON->new;

	my $db = $self->app->pg->db;

	for my $entry (
		$db->select( 'in_transit_str', '*', { cancelled => 0 } )->hashes->each )
	{

		my $uid      = $entry->{user_id};
		my $dep      = $entry->{dep_eva};
		my $arr      = $entry->{arr_eva};
		my $train_id = $entry->{train_id};

		# Note: IRIS data is not always updated in real-time. Both departure and
		# arrival delays may take several minutes to appear, especially in case
		# of large-scale disturbances. We work around this by continuing to
		# update departure data for up to 15 minutes after departure and
		# delaying automatic checkout by at least 10 minutes.

		eval {
			if ( $now->epoch - $entry->{real_dep_ts} < 900 ) {
				my $status = $self->app->get_departures( $dep, 30, 30 );
				if ( $status->{errstr} ) {
					die("get_departures($dep): $status->{errstr}\n");
				}

				my ($train) = List::Util::first { $_->train_id eq $train_id }
				@{ $status->{results} };

				if ( not $train ) {
					die("could not find train $train_id at $dep\n");
				}

				# selecting on user_id and train_no avoids a race condition when
				# a user checks into a new train while we are fetching data for
				# their previous journey. In this case, the new train would
				# receive data from the previous journey.
				$db->update(
					'in_transit',
					{
						dep_platform   => $train->platform,
						real_departure => $train->departure,
						route =>
						  $json->encode( [ $self->app->route_diff($train) ] ),
						messages => $json->encode(
							[
								map { [ $_->[0]->epoch, $_->[1] ] }
								  $train->messages
							]
						),
					},
					{
						user_id  => $uid,
						train_no => $train->train_no
					}
				);
				if ( $train->departure_is_cancelled and $arr ) {

					# depending on the amount of users in transit, some time may
					# have passed between fetching $entry from the database and
					# now. Ensure that the user is still checked into this train
					# before calling checkout to mark the cancellation.
					if (
						$db->select(
							'in_transit',
							'count(*) as count',
							{
								user_id             => $uid,
								train_no            => $train->train_no,
								checkin_station_id  => $dep,
								checkout_station_id => $arr,
							}
						)->hash->{count}
					  )
					{
						$db->update(
							'in_transit',
							{
								cancelled => 1,
							},
							{
								user_id             => $uid,
								train_no            => $train->train_no,
								checkin_station_id  => $dep,
								checkout_station_id => $arr,
							}
						);

                  # check out (adds a cancelled journey and resets journey state
                  # to checkin
						$self->app->checkout( $arr, 1, $uid );
					}
				}
				else {
					$self->app->add_route_timestamps( $uid, $train, 1 );
				}
			}
		};
		if ($@) {
			$self->app->log->error("work($uid)/departure: $@");
		}

		eval {
			if (
				$arr
				and ( not $entry->{real_arr_ts}
					or $now->epoch - $entry->{real_arr_ts} < 600 )
			  )
			{
				my $status = $self->app->get_departures( $arr, 20, 220 );
				if ( $status->{errstr} ) {
					die("get_departures($arr): $status->{errstr}\n");
				}

				# Note that a train may pass the same station several times.
				# Notable example: S41 / S42 ("Ringbahn") both starts and
				# terminates at Berlin Südkreuz
				my ($train) = List::Util::first {
					$_->train_id eq $train_id
					  and $_->sched_arrival
					  and $_->sched_arrival->epoch > $entry->{sched_dep_ts}
				}
				@{ $status->{results} };

				$train //= List::Util::first { $_->train_id eq $train_id }
				@{ $status->{results} };

				if ( not $train ) {

					# If we haven't seen the train yet, its arrival is probably
					# too far in the future. This is not critical.
					return;
				}

             # selecting on user_id, train_no and checkout_station_id avoids a
             # race condition when a user checks into a new train or changes
             # their destination station while we are fetching times based on no
             # longer valid database entries.
				$db->update(
					'in_transit',
					{
						arr_platform  => $train->platform,
						sched_arrival => $train->sched_arrival,
						real_arrival  => $train->arrival,
						route =>
						  $json->encode( [ $self->app->route_diff($train) ] ),
						messages => $json->encode(
							[
								map { [ $_->[0]->epoch, $_->[1] ] }
								  $train->messages
							]
						),
					},
					{
						user_id             => $uid,
						train_no            => $train->train_no,
						checkout_station_id => $arr
					}
				);
				if ( $train->arrival_is_cancelled ) {

					# depending on the amount of users in transit, some time may
					# have passed between fetching $entry from the database and
					# now. Ensure that the user is still checked into this train
					# before calling checkout to mark the cancellation.
					if (
						$db->select(
							'in_transit',
							'count(*) as count',
							{
								user_id             => $uid,
								train_no            => $train->train_no,
								checkout_station_id => $arr
							}
						)->hash->{count}
					  )
					{
                  # check out (adds a cancelled journey and resets journey state
                  # to destination selection)
						$self->app->checkout( $arr, 0, $uid );
					}
				}
				else {
					$self->app->add_route_timestamps( $uid, $train, 0 );
				}
			}
			elsif ( $entry->{real_arr_ts} ) {
				my ( undef, $error ) = $self->app->checkout( $arr, 1, $uid );
				if ($error) {
					die("${error}\n");
				}
			}
		};
		if ($@) {
			$self->app->log->error("work($uid)/arrival: $@");
		}

		eval { }
	}

	for my $traewelling ( $self->app->get_traewelling_pull_accounts ) {
		$self->app->log->debug(
			"Pulling Traewelling status for UID $traewelling->{user_id}");
		$self->app->traewelling_get_status_p( $traewelling->{data}{user_name},
			$traewelling->{token} )->then(
			sub {
				my ($status) = @_;
				if ( not $status->{checkin}
					or $now->epoch - $status->{checkin}->epoch > 900 )
				{
					return;
				}
				# TODO Protokollieren: Letzter behandelter Status ist ID X -> muss nicht nochmal angeschaut werden
				$self->app->log->debug("... user is checked in:");
				while ( my ( $k, $v ) = each %{$status} ) {
					$self->app->log->debug("$k = $v");
				}
				my $user = $self->app->get_user_status($traewelling->{user_id});
				if ($user->{dep_eva} == $status->{dep_eva} and $user->{sched_departure}->epoch == $status->{dep_dt}->epoch) {
					$self->app->log->debug("... user is also checked in via travelynx. nothing to do.");
					continue;
				}
				my $dep
				  = $self->app->get_departures( $status->{dep_eva}, 60, 40, 0 );
				if ( $dep->{errstr} ) {
					$self->app->trwl_log($traewelling->{user_id},
						"Fehler bei Traewelling-Status $status->{status_id} ($status->{line} nach $status->{arr_name}): $dep->{errstr}", 1);
					return;
				}
				my $train_id;
				for my $train ( @{ $dep->{results} } ) {
					if ( $train->line ne $status->{line} ) {
						next;
					}
					if ( not $train->sched_departure or $train->sched_departure->epoch
						!= $status->{dep_dt}->epoch )
					{
						next;
					}
					if (
						not List::Util::first { $_ eq $status->{arr_name} }
						$train->route_post
					  )
					{
						next;
					}
					$train_id = $train->train_id;
				}
				if ($train_id) {
					# TODO Transaktion -> nur committen, wenn checkin und checkout erfolgreich
					# my $db = $self->app->pg->db;
					# my $tx = $db->begin;
					my ( undef, $err )
					  = $self->app->checkin( $status->{dep_eva}, $train_id,
						$traewelling->{user_id} );
					if ( not $err ) {
						( undef, $err )
						  = $self->app->checkout( $status->{arr_eva}, 0,
							$traewelling->{user_id} );
						if (not $err) {
							$self->app->trwl_log($traewelling->{user_id},
								"Traewelling-Status $status->{status_id} übernommen: Eingecheckt in $status->{line} nach $status->{arr_name} (Zug-ID $train_id)", 0);
							#$tx->commit;
						}
					}
					if ($err) {
						$self->app->trwl_log($traewelling->{user_id},
							"Fehler bei Traewelling-Status $status->{status_id} ($status->{line} nach $status->{arr_name}, Zug-ID $train_id): $err", 1);
					}
				}
				else {
					my $is_error = 0;
					if ($status->{category} =~ m{^ (?: nationalExpress | regional | suburban ) $ }x) {
						$is_error = 1;
					}
					$self->app->trwl_log($traewelling->{user_id},
						"Traewelling-Status $status->{status_id} ($status->{line} nach $status->{arr_name}): Kein Zug gefunden", $is_error);
				}
			}
		)->catch(
			sub {
				my ($err) = @_;
				$self->app->log->warn(
					"traewelling_get_status_p($traewelling->{user_id}): $err");
			}
		)->wait;
	}

	# Computing yearly stats may take a while, but we've got all time in the
	# world here. This means users won't have to wait when loading their
	# own by-year journey log.
	for my $user ( $db->select( 'users', 'id', { status => 1 } )->hashes->each )
	{
		$self->app->get_journey_stats(
			uid  => $user->{id},
			year => $now->year
		);
	}
}

1;

__END__

=head1 SYNOPSIS

  Usage: index.pl work

  Work Work Work.

  Should be called from a cronjob every three minutes or so.
