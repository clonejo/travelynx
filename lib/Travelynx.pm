package Travelynx;
use Mojo::Base 'Mojolicious';

use Mojo::Pg;
use Mojolicious::Plugin::Authentication;
use Cache::File;
use Crypt::Eksblowfish::Bcrypt qw(bcrypt en_base64);
use DateTime;
use Encode qw(decode encode);
use Geo::Distance;
use JSON;
use List::Util qw(first);
use List::MoreUtils qw(after_incl before_incl);
use Travel::Status::DE::IRIS;
use Travel::Status::DE::IRIS::Stations;
use Travelynx::Helper::Sendmail;

sub check_password {
	my ( $password, $hash ) = @_;

	if ( bcrypt( $password, $hash ) eq $hash ) {
		return 1;
	}
	return 0;
}

sub epoch_to_dt {
	my ($epoch) = @_;

	# Bugs (and user errors) may lead to undefined timestamps. Set them to
	# 1970-01-01 to avoid crashing and show obviously wrong data instead.
	$epoch //= 0;

	return DateTime->from_epoch(
		epoch     => $epoch,
		time_zone => 'Europe/Berlin'
	);
}

sub get_station {
	my ($station_name) = @_;

	my @candidates
	  = Travel::Status::DE::IRIS::Stations::get_station($station_name);

	if ( @candidates == 1 ) {
		return $candidates[0];
	}
	return undef;
}

sub startup {
	my ($self) = @_;

	push( @{ $self->commands->namespaces }, 'Travelynx::Command' );

	$self->defaults( layout => 'default' );

	$self->types->type( json => 'application/json; charset=utf-8' );

	$self->plugin('Config');

	if ( $self->config->{secrets} ) {
		$self->secrets( $self->config->{secrets} );
	}

	$self->plugin(
		authentication => {
			autoload_user => 1,
			fail_render   => { template => 'login' },
			load_user     => sub {
				my ( $self, $uid ) = @_;
				return $self->get_user_data($uid);
			},
			validate_user => sub {
				my ( $self, $username, $password, $extradata ) = @_;
				my $user_info = $self->get_user_password($username);
				if ( not $user_info ) {
					return undef;
				}
				if ( $user_info->{status} != 1 ) {
					return undef;
				}
				if ( check_password( $password, $user_info->{password_hash} ) )
				{
					return $user_info->{id};
				}
				return undef;
			},
		}
	);
	$self->sessions->default_expiration( 60 * 60 * 24 * 180 );

	$self->defaults( layout => 'default' );

	$self->attr(
		cache_iris_main => sub {
			my ($self) = @_;

			return Cache::File->new(
				cache_root      => $self->app->config->{cache}->{schedule},
				default_expires => '6 hours',
				lock_level      => Cache::File::LOCK_LOCAL(),
			);
		}
	);

	$self->attr(
		cache_iris_rt => sub {
			my ($self) = @_;

			return Cache::File->new(
				cache_root      => $self->app->config->{cache}->{realtime},
				default_expires => '70 seconds',
				lock_level      => Cache::File::LOCK_LOCAL(),
			);
		}
	);

	$self->attr(
		action_type => sub {
			return {
				checkin        => 1,
				checkout       => 2,
				cancelled_from => 4,
				cancelled_to   => 5,
			};
		}
	);
	$self->attr(
		action_types => sub {
			return [qw(checkin checkout undo cancelled_from cancelled_to)];
		}
	);
	$self->attr(
		token_type => sub {
			return {
				status  => 1,
				history => 2,
				action  => 3,
			};
		}
	);
	$self->attr(
		token_types => sub {
			return [qw(status history action)];
		}
	);

	$self->helper(
		sendmail => sub {
			state $sendmail = Travelynx::Helper::Sendmail->new(
				config => ( $self->config->{mail} // {} ),
				log    => $self->log
			);
		}
	);

	$self->helper(
		pg => sub {
			my ($self) = @_;
			my $config = $self->app->config;

			my $dbname = $config->{db}->{database};
			my $host   = $config->{db}->{host} // 'localhost';
			my $port   = $config->{db}->{port} // 5432;
			my $user   = $config->{db}->{user};
			my $pw     = $config->{db}->{password};

			state $pg
			  = Mojo::Pg->new("postgresql://${user}\@${host}:${port}/${dbname}")
			  ->password($pw);
		}
	);

	$self->helper(
		'numify_skipped_stations' => sub {
			my ( $self, $count ) = @_;

			if ( $count == 0 ) {
				return 'INTERNAL ERROR';
			}
			if ( $count == 1 ) {
				return
'Eine Station ohne Geokoordinaten wurde nicht berücksichtigt.';
			}
			return
"${count} Stationen ohne Geookordinaten wurden nicht berücksichtigt.";
		}
	);

	$self->helper(
		'get_departures' => sub {
			my ( $self, $station, $lookbehind ) = @_;

			$lookbehind //= 180;

			my @station_matches
			  = Travel::Status::DE::IRIS::Stations::get_station($station);

			if ( @station_matches == 1 ) {
				$station = $station_matches[0][0];
				my $status = Travel::Status::DE::IRIS->new(
					station        => $station,
					main_cache     => $self->app->cache_iris_main,
					realtime_cache => $self->app->cache_iris_rt,
					lookbehind     => 20,
					datetime => DateTime->now( time_zone => 'Europe/Berlin' )
					  ->subtract( minutes => $lookbehind ),
					lookahead => $lookbehind + 10,
				);
				return {
					results       => [ $status->results ],
					errstr        => $status->errstr,
					station_ds100 => (
						$status->station ? $status->station->{ds100} : 'undef'
					),
					station_name =>
					  ( $status->station ? $status->station->{name} : 'undef' ),
				};
			}
			elsif ( @station_matches > 1 ) {
				return {
					results => [],
					errstr  => 'Ambiguous station name',
				};
			}
			else {
				return {
					results => [],
					errstr  => 'Unknown station name',
				};
			}
		}
	);

	# Returns (checkin id, checkout id, error)
	# Must be called during a transaction.
	# Must perform a rollback on error.
	$self->helper(
		'add_journey' => sub {
			my ( $self, %opt ) = @_;

			my $user_status = $self->get_user_status;
			if ( $user_status->{checked_in} or $user_status->{cancelled} ) {

            # TODO: change database schema to one row per journey instead of two
				return ( undef, undef,
'Während einer Zugfahrt können momentan keine manuellen Einträge vorgenommen werden. Klingt komisch, ist aber so.'
				);
			}

			my $uid         = $self->current_user->{id};
			my $dep_station = get_station( $opt{dep_station} );
			my $arr_station = get_station( $opt{arr_station} );

			if ( not $dep_station ) {
				return ( undef, undef, 'Unbekannter Startbahnhof' );
			}
			if ( not $arr_station ) {
				return ( undef, undef, 'Unbekannter Zielbahnhof' );
			}

			my $checkin_id;
			my $checkout_id;

			eval {
				$checkin_id = $self->pg->db->insert(
					'user_actions',
					{
						user_id    => $uid,
						action_id  => $self->app->action_type->{checkin},
						station_id => $self->get_station_id(
							ds100 => $dep_station->[0],
							name  => $dep_station->[1],
						),
						action_time =>
						  DateTime->now( time_zone => 'Europe/Berlin' ),
						edited     => 0x0f,
						train_type => $opt{train_type},
						train_line => $opt{train_line},
						train_no   => $opt{train_no},
						sched_time => $opt{sched_departure},
						real_time  => $opt{rt_departure},
					},
					{ returning => 'id' }
				)->hash->{id};
			};

			if ($@) {
				$self->app->log->error(
					"add_journey($uid, checkin): INSERT failed: $@");
				return ( undef, undef, 'INSERT failed: ' . $@ );
			}

			eval {
				$checkout_id = $self->pg->db->insert(
					'user_actions',
					{
						user_id    => $uid,
						action_id  => $self->app->action_type->{checkout},
						station_id => $self->get_station_id(
							ds100 => $arr_station->[0],
							name  => $arr_station->[1],
						),
						action_time =>
						  DateTime->now( time_zone => 'Europe/Berlin' ),
						edited     => 0x0f,
						train_type => $opt{train_type},
						train_line => $opt{train_line},
						train_no   => $opt{train_no},
						sched_time => $opt{sched_arrival},
						real_time  => $opt{rt_arrival},
					},
					{ returnning => 'id' }
				)->hash->{id};
			};
			if ($@) {
				$self->app->log->error(
					"add_journey($uid, checkout): INSERT failed: $@");
				return ( undef, undef, 'INSERT failed: ' . $@ );
			}
			return ( $checkin_id, $checkout_id, undef );
		}
	);

	$self->helper(
		'checkin' => sub {
			my ( $self, $station, $train_id, $action_id ) = @_;

			$action_id //= $self->app->action_type->{checkin};

			my $status = $self->get_departures($station);
			if ( $status->{errstr} ) {
				return ( undef, $status->{errstr} );
			}
			else {
				my ($train)
				  = first { $_->train_id eq $train_id } @{ $status->{results} };
				if ( not defined $train ) {
					return ( undef, "Train ${train_id} not found" );
				}
				else {

					my $user = $self->get_user_status;
					if ( $user->{checked_in} ) {

                # If a user is already checked in, we assume that they forgot to
                # check out and do it for them.
						$self->checkout( $station, 1 );
					}
					elsif ( $user->{cancelled} ) {

						# Same
						$self->checkout( $station, 1,
							$self->app->action_type->{cancelled_to} );
					}

					eval {
						$self->pg->db->insert(
							'user_actions',
							{
								user_id    => $self->current_user->{id},
								action_id  => $action_id,
								station_id => $self->get_station_id(
									ds100 => $status->{station_ds100},
									name  => $status->{station_name}
								),
								action_time =>
								  DateTime->now( time_zone => 'Europe/Berlin' ),
								edited     => 0,
								train_type => $train->type,
								train_line => $train->line_no,
								train_no   => $train->train_no,
								train_id   => $train->train_id,
								sched_time => $train->sched_departure,
								real_time  => $train->departure,
								route      => join( '|', $train->route ),
								messages   => join(
									'|',
									map {
										( $_->[0] ? $_->[0]->epoch : q{} ) . ':'
										  . $_->[1]
									} $train->messages
								)
							}
						);
					};
					if ($@) {
						my $uid = $self->current_user->{id};
						$self->app->log->error(
							"Checkin($uid, $action_id): INSERT failed: $@");
						return ( undef, 'INSERT failed: ' . $@ );
					}
					return ( $train, undef );
				}
			}
		}
	);

	$self->helper(
		'undo' => sub {
			my ( $self, $action_id ) = @_;

			my $status = $self->get_user_status;

			if ( $action_id < 1 or $status->{action_id} != $action_id ) {
				return
"Invalid action ID: $action_id != $status->{action_id}. Note that you can only undo your latest action.";
			}

			eval {
				$self->pg->db->delete( 'user_actions', { id => $action_id } );
			};
			if ($@) {
				my $uid = $self->current_user->{id};
				$self->app->log->error(
					"Undo($uid, $action_id): DELETE failed: $@");
				return 'DELETE failed: ' . $@;
			}
			return;
		}
	);

	$self->helper(
		'invalidate_stats_cache' => sub {
			my ( $self, $ts ) = @_;

			my $uid = $self->current_user->{id};
			$ts //= DateTime->now( time_zone => 'Europe/Berlin' );

			# ts is the checkout timestamp or (for manual entries) the
			# time of arrival. As the journey may span a month or year boundary,
			# there is a total of five cache entries we need to invalidate:
			# * year, month
			# * year
			# * (year, month) - 1 month   (with wraparound)
			# * (year) - 1 year
			# * total stats

			$self->pg->db->delete(
				'journey_stats',
				{
					user_id => $uid,
					year    => $ts->year,
					month   => $ts->month,
				}
			);
			$self->pg->db->delete(
				'journey_stats',
				{
					user_id => $uid,
					year    => $ts->year,
					month   => 0,
				}
			);
			$ts->subtract( months => 1 );
			$self->pg->db->delete(
				'journey_stats',
				{
					user_id => $uid,
					year    => $ts->year,
					month   => $ts->month,
				}
			);
			$ts->subtract( months => 11 );
			$self->pg->db->delete(
				'journey_stats',
				{
					user_id => $uid,
					year    => $ts->year,
					month   => 0,
				}
			);
			$self->pg->db->delete(
				'journey_stats',
				{
					user_id => $uid,
					year    => 0,
					month   => 0,
				}
			);
		}
	);

	$self->helper(
		'checkout' => sub {
			my ( $self, $station, $force, $action_id ) = @_;

			$action_id //= $self->app->action_type->{checkout};

			my $status   = $self->get_departures( $station, 180 );
			my $user     = $self->get_user_status;
			my $train_id = $user->{train_id};

			if ( not $user->{checked_in} and not $user->{cancelled} ) {
				return 'You are not checked into any train';
			}
			if ( $status->{errstr} and not $force ) {
				return $status->{errstr};
			}

			my $now = DateTime->now( time_zone => 'Europe/Berlin' );
			my ($train)
			  = first { $_->train_id eq $train_id } @{ $status->{results} };
			if ( not defined $train ) {
				if ($force) {
					eval {
						$self->pg->db->insert(
							'user_actions',
							{
								user_id    => $self->current_user->{id},
								action_id  => $action_id,
								station_id => $self->get_station_id(
									ds100 => $status->{station_ds100},
									name  => $status->{station_name}
								),
								action_time => $now,
								edited      => 0
							}
						);
					};
					if ($@) {
						my $uid = $self->current_user->{id};
						$self->app->log->error(
"Force checkout($uid, $action_id): INSERT failed: $@"
						);
						return 'INSERT failed: ' . $@;
					}
					$self->invalidate_stats_cache;
					return;
				}
				else {
					return "Train ${train_id} not found";
				}
			}
			else {
				eval {
					$self->pg->db->insert(
						'user_actions',
						{
							user_id    => $self->current_user->{id},
							action_id  => $action_id,
							station_id => $self->get_station_id(
								ds100 => $status->{station_ds100},
								name  => $status->{station_name}
							),
							action_time => $now,
							edited      => 0,
							train_type  => $train->type,
							train_line  => $train->line_no,
							train_no    => $train->train_no,
							train_id    => $train->train_id,
							sched_time  => $train->sched_arrival,
							real_time   => $train->arrival,
							route       => join( '|', $train->route ),
							messages    => join(
								'|',
								map {
									( $_->[0] ? $_->[0]->epoch : q{} ) . ':'
									  . $_->[1]
								} $train->messages
							)
						}
					);
				};
				if ($@) {
					my $uid = $self->current_user->{id};
					$self->app->log->error(
						"Checkout($uid, $action_id): INSERT failed: $@");
					return 'INSERT failed: ' . $@;
				}
				$self->invalidate_stats_cache;
				return;
			}
		}
	);

	$self->helper(
		'update_journey_part' => sub {
			my ( $self, $db, $checkin_id, $checkout_id, $key, $value ) = @_;
			my $rows;

			eval {
				if ( $key eq 'sched_departure' ) {
					$rows = $db->update(
						'user_actions',
						{
							sched_time => $value,
						},
						{
							id        => $checkin_id,
							action_id => $self->app->action_type->{checkin},
						}
					)->rows;
				}
				elsif ( $key eq 'rt_departure' ) {
					$rows = $db->update(
						'user_actions',
						{
							real_time => $value,
						},
						{
							id        => $checkin_id,
							action_id => $self->app->action_type->{checkin},
						}
					)->rows;
				}
				elsif ( $key eq 'sched_arrival' ) {
					$rows = $db->update(
						'user_actions',
						{
							sched_time => $value,
						},
						{
							id        => $checkout_id,
							action_id => $self->app->action_type->{checkout},
						}
					)->rows;
				}
				elsif ( $key eq 'rt_arrival' ) {
					$rows = $db->update(
						'user_actions',
						{
							real_time => $value,
						},
						{
							id        => $checkout_id,
							action_id => $self->app->action_type->{checkout},
						}
					)->rows;
				}
				else {
					$self->app->log->error(
"update_journey_part($checkin_id, $checkout_id): Invalid key $key"
					);
				}
			};

			if ($@) {
				$self->app->log->error(
"update_journey_part($checkin_id, $checkout_id): UPDATE failed: $@"
				);
				return 'UPDATE failed: ' . $@;
			}
			if ( $rows == 1 ) {
				return undef;
			}
			return 'UPDATE failed: did not match any journey part';
		}
	);

	$self->helper(
		'journey_sanity_check' => sub {
			my ( $self, $journey ) = @_;

			if ( $journey->{sched_duration} and $journey->{sched_duration} < 0 )
			{
				return
'Die geplante Dauer dieser Zugfahrt ist negativ. Zeitreisen werden aktuell nicht unterstützt.';
			}
			if ( $journey->{rt_duration} and $journey->{rt_duration} < 0 ) {
				return
'Die Dauer dieser Zugfahrt ist negativ. Zeitreisen werden aktuell nicht unterstützt.';
			}
			if (    $journey->{sched_duration}
				and $journey->{sched_duration} > 60 * 60 * 24 )
			{
				return 'Die Zugfahrt ist länger als 24 Stunden.';
			}
			if (    $journey->{rt_duration}
				and $journey->{rt_duration} > 60 * 60 * 24 )
			{
				return 'Die Zugfahrt ist länger als 24 Stunden.';
			}

			return undef;
		}
	);

	$self->helper(
		'get_station_id' => sub {
			my ( $self, %opt ) = @_;

			my $res = $self->pg->db->select( 'stations', ['id'],
				{ ds100 => $opt{ds100} } );
			my $res_h = $res->hash;

			if ($res_h) {
				$res->finish;
				return $res_h->{id};
			}

			$self->pg->db->insert(
				'stations',
				{
					ds100 => $opt{ds100},
					name  => $opt{name},
				}
			);
			$res = $self->pg->db->select( 'stations', ['id'],
				{ ds100 => $opt{ds100} } );
			my $id = $res->hash->{id};
			$res->finish;
			return $id;
		}
	);

	$self->helper(
		'get_user_token' => sub {
			my ( $self, $uid ) = @_;

			my $res = $self->pg->db->select(
				'users',
				[ 'name', 'status', 'token' ],
				{ id => $uid }
			);

			if ( my $ret = $res->array ) {
				return @{$ret};
			}
			return;
		}
	);

	# This helper should only be called directly when also providing a user ID.
	# If you don't have one, use current_user() instead (get_user_data will
	# delegate to it anyways).
	$self->helper(
		'get_user_data' => sub {
			my ( $self, $uid ) = @_;

			$uid //= $self->current_user->{id};

			my $user_data = $self->pg->db->select(
				'users',
				'id, name, status, public_level, email, '
				  . 'extract(epoch from registered_at) as registered_at_ts, '
				  . 'extract(epoch from last_login) as last_login_ts, '
				  . 'extract(epoch from deletion_requested) as deletion_requested_ts',
				{ id => $uid }
			)->hash;

			if ($user_data) {
				return {
					id            => $user_data->{id},
					name          => $user_data->{name},
					status        => $user_data->{status},
					is_public     => $user_data->{public_level},
					email         => $user_data->{email},
					registered_at => DateTime->from_epoch(
						epoch     => $user_data->{registered_at_ts},
						time_zone => 'Europe/Berlin'
					),
					last_seen => DateTime->from_epoch(
						epoch     => $user_data->{last_login_ts},
						time_zone => 'Europe/Berlin'
					),
					deletion_requested => $user_data->{deletion_requested_ts}
					? DateTime->from_epoch(
						epoch     => $user_data->{deletion_requested_ts},
						time_zone => 'Europe/Berlin'
					  )
					: undef,
				};
			}
			return undef;
		}
	);

	$self->helper(
		'get_api_token' => sub {
			my ( $self, $uid ) = @_;
			$uid //= $self->current_user->{id};

			my $token = {};
			my $res   = $self->pg->db->select(
				'tokens',
				[ 'type', 'token' ],
				{ user_id => $uid }
			);

			for my $entry ( $res->hashes->each ) {
				$token->{ $self->app->token_types->[ $entry->{type} - 1 ] }
				  = $entry->{token};
			}

			return $token;
		}
	);

	$self->helper(
		'get_user_password' => sub {
			my ( $self, $name ) = @_;

			my $res_h = $self->pg->db->select(
				'users',
				'id, name, status, password as password_hash',
				{ name => $name }
			)->hash;

			return $res_h;
		}
	);

	$self->helper(
		'add_user' => sub {
			my ( $self, $db, $user_name, $email, $token, $password ) = @_;

          # This helper must be called during a transaction, as user creation
          # may fail even after the database entry has been generated, e.g.  if
          # the registration mail cannot be sent. We therefore use $db (the
          # database handle performing the transaction) instead of $self->pg->db
          # (which may be a new handle not belonging to the transaction).

			my $now = DateTime->now( time_zone => 'Europe/Berlin' );

			my $res = $db->insert(
				'users',
				{
					name          => $user_name,
					status        => 0,
					public_level  => 0,
					email         => $email,
					token         => $token,
					password      => $password,
					registered_at => $now,
					last_login    => $now,
				},
				{ returning => 'id' }
			);

			return $res->hash->{id};
		}
	);

	$self->helper(
		'flag_user_deletion' => sub {
			my ( $self, $uid ) = @_;

			my $now = DateTime->now( time_zone => 'Europe/Berlin' );

			$self->pg->db->update(
				'users',
				{ deletion_requested => $now },
				{
					id => $uid,
				}
			);
		}
	);

	$self->helper(
		'unflag_user_deletion' => sub {
			my ( $self, $uid ) = @_;

			$self->pg->db->update(
				'users',
				{
					deletion_requested => undef,
				},
				{
					id => $uid,
				}
			);
		}
	);

	$self->helper(
		'set_user_password' => sub {
			my ( $self, $uid, $password ) = @_;

			$self->pg->db->update(
				'users',
				{ password => $password },
				{ id       => $uid }
			);
		}
	);

	$self->helper(
		'check_if_user_name_exists' => sub {
			my ( $self, $user_name ) = @_;

			my $count = $self->pg->db->select(
				'users',
				'count(*) as count',
				{ name => $user_name }
			)->hash->{count};

			if ($count) {
				return 1;
			}
			return 0;
		}
	);

	$self->helper(
		'check_if_mail_is_blacklisted' => sub {
			my ( $self, $mail ) = @_;

			my $count = $self->pg->db->select(
				'users',
				'count(*) as count',
				{
					email  => $mail,
					status => 0,
				}
			)->hash->{count};

			if ($count) {
				return 1;
			}

			$count = $self->pg->db->select(
				'pending_mails',
				'count(*) as count',
				{
					email     => $mail,
					num_tries => { '>', 1 },
				}
			)->hash->{count};

			if ($count) {
				return 1;
			}
			return 0;
		}
	);

	$self->helper(
		'delete_journey' => sub {
			my ( $self, $checkin_id, $checkout_id, $checkin_epoch,
				$checkout_epoch )
			  = @_;
			my $uid = $self->current_user->{id};

			my @journeys = $self->get_user_travels(
				uid         => $uid,
				checkout_id => $checkout_id
			);
			if ( @journeys == 0 ) {
				return 'Journey not found';
			}
			my $journey = $journeys[0];

			# Double-check (comparing both ID and action epoch) to make sure we
			# are really deleting the right journey and the user isn't just
			# playing around with POST requests.
			if (   $journey->{ids}[0] != $checkin_id
				or $journey->{ids}[1] != $checkout_id
				or $journey->{checkin}->epoch != $checkin_epoch
				or $journey->{checkout}->epoch != $checkout_epoch )
			{
				return 'Invalid journey data';
			}

			my $rows;
			eval {
				$rows = $self->pg->db->delete(
					'user_actions',
					{
						user_id => $uid,
						id      => [ $checkin_id, $checkout_id ]
					}
				)->rows;
			};

			if ($@) {
				$self->app->log->error(
					"Delete($uid, $checkin_id, $checkout_id): DELETE failed: $@"
				);
				return 'DELETE failed: ' . $@;
			}

			if ( $rows == 2 ) {
				$self->invalidate_stats_cache( $journey->{checkout} );
				return undef;
			}
			return sprintf( 'Deleted %d rows, expected 2', $rows );
		}
	);

	$self->helper(
		'get_journey_stats' => sub {
			my ( $self, %opt ) = @_;

			if ( $opt{cancelled} ) {
				$self->app->log->warning(
'get_journey_stats called with illegal option cancelled => 1'
				);
				return {};
			}

			my $uid   = $self->current_user->{id};
			my $year  = $opt{year} // 0;
			my $month = $opt{month} // 0;

			# Assumption: If the stats cache contains an entry it is up-to-date.
			# -> Cache entries must be explicitly invalidated whenever the user
			# checks out of a train or manually edits/adds a journey.

			my $res = $self->pg->db->select(
				'journey_stats',
				['data'],
				{
					user_id => $uid,
					year    => $year,
					month   => $month
				}
			);

			my $res_h = $res->expand->hash;

			if ($res_h) {
				$res->finish;
				return $res_h->{data};
			}

			my $interval_start = DateTime->new(
				time_zone => 'Europe/Berlin',
				year      => 2000,
				month     => 1,
				day       => 1,
				hour      => 0,
				minute    => 0,
				second    => 0,
			);

          # I wonder if people will still be traveling by train in the year 3000
			my $interval_end = $interval_start->clone->add( years => 1000 );

			if ( $opt{year} and $opt{month} ) {
				$interval_start->set(
					year  => $opt{year},
					month => $opt{month}
				);
				$interval_end = $interval_start->clone->add( months => 1 );
			}
			elsif ( $opt{year} ) {
				$interval_start->set( year => $opt{year} );
				$interval_end = $interval_start->clone->add( years => 1 );
			}

			my @journeys = $self->get_user_travels(
				cancelled => $opt{cancelled} ? 1 : 0,
				verbose   => 1,
				after     => $interval_start,
				before    => $interval_end
			);
			my $stats = $self->compute_journey_stats(@journeys);

			$self->pg->db->insert(
				'journey_stats',
				{
					user_id => $uid,
					year    => $year,
					month   => $month,
					data    => JSON->new->encode($stats),
				}
			);

			return $stats;
		}
	);

	$self->helper(
		'get_user_travels' => sub {
			my ( $self, %opt ) = @_;

			my $uid = $opt{uid} || $self->current_user->{id};

			# If get_user_travels is called from inside a transaction, db
			# specifies the database handle performing the transaction.
			# Otherwise, we grab a fresh one.
			my $db = $opt{db} // $self->pg->db;

			my $selection = qq{
			user_actions.id as action_log_id, action_id,
			extract(epoch from action_time) as action_time_ts,
			stations.ds100 as ds100, stations.name as name,
			train_type, train_line, train_no, train_id,
			extract(epoch from sched_time) as sched_time_ts,
			extract(epoch from real_time) as real_time_ts,
			route, messages, edited
			};
			$selection =~ tr{\n}{}d;
			my %where = ( user_id => $uid );
			my %order = (
				order_by => {
					-desc => 'action_time',
				}
			);

			if ( $opt{limit} ) {
				$order{limit} = 10;
			}

			if ( $opt{checkout_id} ) {
				$where{'user_actions.id'} = { '<=', $opt{checkout_id} };
				$order{limit} = 2;
			}
			elsif ( $opt{after} and $opt{before} ) {

         # Each journey consists of exactly two database entries: one for
         # checkin, one for checkout. A simple query using e.g.
         # after = YYYY-01-01T00:00:00 and before YYYY-02-01T00:00:00
         # will miss journeys where checkin and checkout take place in
         # different months.
         # We therefore add one day to the before timestamp and filter out
         # journeys whose checkin lies outside the originally requested
         # time range afterwards.
         # For an additional twist, get_interval_actions_query filters based
         # on the action time, not actual departure, as force
         # checkout actions lack sched_time and real_time data. By
         # subtracting one day from "after" (i.e., moving it one day into
         # the past), we make sure not to miss journeys where the real departure
         # time falls into the interval, but the checkin time does not.
         # Again, this is addressed in postprocessing at the bottom of this
         # helper.
         # This works under the assumption that there are no DB trains whose
         # journey takes more than 24 hours. If this no longer holds,
         # please adjust the intervals accordingly.
				$where{action_time} = {
					-between => [
						$opt{after}->clone->subtract( days => 1 ),
						$opt{before}->clone->add( days => 1 )
					]
				};
			}

			my @match_actions = (
				$self->app->action_type->{checkout},
				$self->app->action_type->{checkin}
			);
			if ( $opt{cancelled} ) {
				@match_actions = (
					$self->app->action_type->{cancelled_to},
					$self->app->action_type->{cancelled_from}
				);
			}

			my @travels;
			my $prev_action = 0;

			my $res = $db->select(
				[
					'user_actions',
					[
						-left => 'stations',
						id    => 'station_id'
					]
				],
				$selection,
				\%where,
				\%order
			);

			for my $entry ( $res->hashes->each ) {

				if ( $entry->{action_id} == $match_actions[0]
					or ( $opt{checkout_id} and not @travels ) )
				{
					push(
						@travels,
						{
							ids     => [ undef, $entry->{action_log_id} ],
							to_name => $entry->{name},
							sched_arrival =>
							  epoch_to_dt( $entry->{sched_time_ts} ),
							rt_arrival => epoch_to_dt( $entry->{real_time_ts} ),
							checkout => epoch_to_dt( $entry->{action_time_ts} ),
							type     => $entry->{train_type},
							line     => $entry->{train_line},
							no       => $entry->{train_no},
							messages => $entry->{messages}
							? [ split( qr{[|]}, $entry->{messages} ) ]
							: undef,
							route => $entry->{route}
							? [ split( qr{[|]}, $entry->{route} ) ]
							: undef,
							completed => 0,
							edited    => $entry->{edited} << 8,
						}
					);
				}
				elsif (
					(
						    $entry->{action_id} == $match_actions[1]
						and $prev_action == $match_actions[0]
					)
					or $opt{checkout_id}
				  )
				{
					my $ref = $travels[-1];
					$ref->{ids}->[0]  = $entry->{action_log_id};
					$ref->{from_name} = $entry->{name};
					$ref->{completed} = 1;
					$ref->{sched_departure}
					  = epoch_to_dt( $entry->{sched_time_ts} );
					$ref->{rt_departure}
					  = epoch_to_dt( $entry->{real_time_ts} );
					$ref->{checkin} = epoch_to_dt( $entry->{action_time_ts} );
					$ref->{type} //= $entry->{train_type};
					$ref->{line} //= $entry->{train_line};
					$ref->{no}   //= $entry->{train_no};
					$ref->{messages}
					  //= [ split( qr{[|]}, $entry->{messages} ) ];
					$ref->{route} //= [ split( qr{[|]}, $entry->{route} ) ];
					$ref->{edited} |= $entry->{edited};

					if ( $opt{verbose} ) {
						my @parsed_messages;
						for my $message ( @{ $ref->{messages} // [] } ) {
							my ( $ts, $msg ) = split( qr{:}, $message );
							push( @parsed_messages,
								[ epoch_to_dt($ts), $msg ] );
						}
						$ref->{messages} = [ reverse @parsed_messages ];
						$ref->{sched_duration}
						  = $ref->{sched_arrival}
						  ? $ref->{sched_arrival}->epoch
						  - $ref->{sched_departure}->epoch
						  : undef;
						$ref->{rt_duration}
						  = $ref->{rt_arrival}
						  ? $ref->{rt_arrival}->epoch
						  - $ref->{rt_departure}->epoch
						  : undef;
						my ( $km, $skip )
						  = $self->get_travel_distance( $ref->{from_name},
							$ref->{to_name}, $ref->{route} );
						$ref->{km_route}   = $km;
						$ref->{skip_route} = $skip;
						( $km, $skip )
						  = $self->get_travel_distance( $ref->{from_name},
							$ref->{to_name},
							[ $ref->{from_name}, $ref->{to_name} ] );
						$ref->{km_beeline}   = $km;
						$ref->{skip_beeline} = $skip;
						my $kmh_divisor
						  = ( $ref->{rt_duration} // $ref->{sched_duration}
							  // 999999 ) / 3600;
						$ref->{kmh_route}
						  = $kmh_divisor ? $ref->{km_route} / $kmh_divisor : -1;
						$ref->{kmh_beeline}
						  = $kmh_divisor
						  ? $ref->{km_beeline} / $kmh_divisor
						  : -1;
					}
					if (    $opt{checkout_id}
						and $entry->{action_id}
						== $self->app->action_type->{cancelled_from} )
					{
						$ref->{cancelled} = 1;
					}
				}
				$prev_action = $entry->{action_id};
			}

			if ( $opt{before} and $opt{after} ) {
				@travels = grep {
					      $_->{rt_departure} >= $opt{after}
					  and $_->{rt_departure} < $opt{before}
				} @travels;
			}

         # user_actions are sorted by action_time. As users are allowed to check
         # into trains in arbitrary order, action_time does not always
         # correspond to departure/arrival time, so we ensure a proper sort
         # order here.
			@travels
			  = sort { $b->{rt_departure} <=> $a->{rt_departure} } @travels;

			return @travels;
		}
	);

	$self->helper(
		'get_journey' => sub {
			my ( $self, %opt ) = @_;

			my @journeys = $self->get_user_travels(%opt);
			if (   @journeys == 0
				or not $journeys[0]{completed}
				or $journeys[0]{ids}[1] != $opt{checkout_id} )
			{
				return undef;
			}

			return $journeys[0];
		}
	);

	$self->helper(
		'get_user_status' => sub {
			my ( $self, $uid ) = @_;

			$uid //= $self->current_user->{id};

			my $selection = qq{
			user_actions.id as action_log_id, action_id,
			extract(epoch from action_time) as action_time_ts,
			stations.ds100 as ds100, stations.name as name,
			train_type, train_line, train_no, train_id,
			extract(epoch from sched_time) as sched_time_ts,
			extract(epoch from real_time) as real_time_ts,
			route
			};
			$selection =~ tr{\n}{}d;

			my $res = $self->pg->db->select(
				[
					'user_actions',
					[
						-left => 'stations',
						id    => 'station_id'
					]
				],
				$selection,
				{
					user_id => $uid,
				},
				{
					order_by => {
						-desc => 'action_time',
					},
					limit => 1,
				}
			);
			my $status = $res->hash;

			if ($status) {
				my $now = DateTime->now( time_zone => 'Europe/Berlin' );

				my $action_ts = epoch_to_dt( $status->{action_time_ts} );
				my $sched_ts  = epoch_to_dt( $status->{sched_time_ts} );
				my $real_ts   = epoch_to_dt( $status->{real_time_ts} );
				my $checkin_station_name = $status->{name};
				my @route = split( qr{[|]}, $status->{route} // q{} );
				my @route_after;
				my $is_after = 0;
				for my $station (@route) {

					if ( $station eq $checkin_station_name ) {
						$is_after = 1;
					}
					if ($is_after) {
						push( @route_after, $station );
					}
				}
				return {
					checked_in => (
						$status->{action_id}
						  == $self->app->action_type->{checkin}
					),
					cancelled => (
						$status->{action_id}
						  == $self->app->action_type->{cancelled_from}
					),
					timestamp       => $action_ts,
					timestamp_delta => $now->epoch - $action_ts->epoch,
					action_id       => $status->{action_log_id},
					sched_ts        => $sched_ts,
					real_ts         => $real_ts,
					station_ds100   => $status->{ds100},
					station_name    => $checkin_station_name,
					train_type      => $status->{train_type},
					train_line      => $status->{train_line},
					train_no        => $status->{train_no},
					train_id        => $status->{train_id},
					route           => \@route,
					route_after     => \@route_after,
				};
			}
			return {
				checked_in => 0,
				timestamp  => epoch_to_dt(0),
				sched_ts   => epoch_to_dt(0),
				real_ts    => epoch_to_dt(0),
			};
		}
	);

	$self->helper(
		'get_travel_distance' => sub {
			my ( $self, $from, $to, $route_ref ) = @_;

			my $distance = 0;
			my $skipped  = 0;
			my $geo      = Geo::Distance->new();
			my @route    = after_incl { $_ eq $from } @{$route_ref};
			@route = before_incl { $_ eq $to } @route;

			if ( @route < 2 ) {

				# I AM ERROR
				return 0;
			}

			my $prev_station = get_station( shift @route );
			if ( not $prev_station ) {
				return 0;
			}

			for my $station_name (@route) {
				if ( my $station = get_station($station_name) ) {
					if ( $#{$prev_station} >= 4 and $#{$station} >= 4 ) {
						$distance
						  += $geo->distance( 'kilometer', $prev_station->[3],
							$prev_station->[4], $station->[3], $station->[4] );
					}
					else {
						$skipped++;
					}
					$prev_station = $station;
				}
			}

			return ( $distance, $skipped );
		}
	);

	$self->helper(
		'compute_journey_stats' => sub {
			my ( $self, @journeys ) = @_;
			my $km_route         = 0;
			my $km_beeline       = 0;
			my $min_travel_sched = 0;
			my $min_travel_real  = 0;
			my $delay_dep        = 0;
			my $delay_arr        = 0;
			my $interchange_real = 0;
			my $num_trains       = 0;
			my $num_journeys     = 0;

			my $next_departure = 0;

			for my $journey (@journeys) {
				$num_trains++;
				$km_route   += $journey->{km_route};
				$km_beeline += $journey->{km_beeline};
				if ( $journey->{sched_duration} > 0 ) {
					$min_travel_sched += $journey->{sched_duration} / 60;
				}
				if ( $journey->{rt_duration} > 0 ) {
					$min_travel_real += $journey->{rt_duration} / 60;
				}
				if ( $journey->{sched_departure} and $journey->{rt_departure} )
				{
					$delay_dep
					  += (  $journey->{rt_departure}->epoch
						  - $journey->{sched_departure}->epoch ) / 60;
				}
				if ( $journey->{sched_arrival} and $journey->{rt_arrival} ) {
					$delay_arr
					  += (  $journey->{rt_arrival}->epoch
						  - $journey->{sched_arrival}->epoch ) / 60;
				}

				# Note that journeys are sorted from recent to older entries
				if (    $journey->{rt_arrival}
					and $next_departure
					and $next_departure - $journey->{rt_arrival}->epoch
					< ( 60 * 60 ) )
				{
					$interchange_real
					  += ( $next_departure - $journey->{rt_arrival}->epoch )
					  / 60;
				}
				else {
					$num_journeys++;
				}
				$next_departure = $journey->{rt_departure}->epoch;
			}
			return {
				km_route             => $km_route,
				km_beeline           => $km_beeline,
				num_trains           => $num_trains,
				num_journeys         => $num_journeys,
				min_travel_sched     => $min_travel_sched,
				min_travel_real      => $min_travel_real,
				min_interchange_real => $interchange_real,
				delay_dep            => $delay_dep,
				delay_arr            => $delay_arr,
			};
		}
	);

	$self->helper(
		'navbar_class' => sub {
			my ( $self, $path ) = @_;

			if ( $self->req->url eq $self->url_for($path) ) {
				return 'active';
			}
			return q{};
		}
	);

	my $r = $self->routes;

	$r->get('/')->to('traveling#homepage');
	$r->get('/about')->to('static#about');
	$r->get('/impressum')->to('static#imprint');
	$r->get('/imprint')->to('static#imprint');
	$r->get('/api/v0/:user_action/:token')->to('api#get_v0');
	$r->get('/login')->to('account#login_form');
	$r->get('/register')->to('account#registration_form');
	$r->get('/reg/:id/:token')->to('account#verify');
	$r->post('/action')->to('traveling#log_action');
	$r->post('/geolocation')->to('traveling#geolocation');
	$r->post('/list_departures')->to('traveling#redirect_to_station');
	$r->post('/login')->to('account#do_login');
	$r->post('/register')->to('account#register');

	my $authed_r = $r->under(
		sub {
			my ($self) = @_;
			if ( $self->is_user_authenticated ) {
				return 1;
			}
			$self->render( 'login', redirect_to => $self->req->url );
			return undef;
		}
	);

	$authed_r->get('/account')->to('account#account');
	$authed_r->get('/cancelled')->to('traveling#cancelled');
	$authed_r->get('/change_password')->to('account#password_form');
	$authed_r->get('/export.json')->to('account#json_export');
	$authed_r->get('/history.json')->to('traveling#json_history');
	$authed_r->get('/history')->to('traveling#history');
	$authed_r->get('/history/:year')->to('traveling#yearly_history');
	$authed_r->get('/history/:year/:month')->to('traveling#monthly_history');
	$authed_r->get('/journey/add')->to('traveling#add_journey_form');
	$authed_r->get('/journey/:id')->to('traveling#journey_details');
	$authed_r->get('/s/*station')->to('traveling#station');
	$authed_r->post('/journey/add')->to('traveling#add_journey_form');
	$authed_r->post('/journey/edit')->to('traveling#edit_journey');
	$authed_r->post('/change_password')->to('account#change_password');
	$authed_r->post('/delete')->to('account#delete');
	$authed_r->post('/logout')->to('account#do_logout');
	$authed_r->post('/set_token')->to('api#set_token');

}

1;
