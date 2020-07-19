package Travelynx::Controller::Traewelling;
use Mojo::Base 'Mojolicious::Controller';
use Mojo::Promise;

sub trwl_getuser_p {
	my ( $self, $uid, $token ) = @_;
	my $ua = $self->ua->request_timeout(20);

	my $header = {
		'User-Agent'    => 'travelynx/' . $self->app->config->{version},
		'Authorization' => "Bearer $token",
	};
	my $promise = Mojo::Promise->new;

	$ua->get_p( "https://traewelling.de/api/v0/getuser" => $header )->then(
		sub {
			my ($tx) = @_;
			if ( my $err = $tx->error ) {
				my $err_msg = "HTTP $err->{code} $err->{message}";
				$promise->reject($err_msg);
			}
			else {
				my $user_data = $tx->result->json;
				$self->mark_trwl_user(
					uid         => $uid,
					trwl_id     => $user_data->{id},
					screen_name => $user_data->{name},
					user_name   => $user_data->{username},
				);
				$promise->resolve;
			}
		}
	)->catch(
		sub {
			my ($err) = @_;
			$promise->reject($err);
		}
	)->wait;

	return $promise;
}

sub trwl_login_p {
	my ( $self, $uid, $email, $password ) = @_;

	my $ua = $self->ua->request_timeout(20);

	my $header
	  = { 'User-Agent' => 'travelynx/' . $self->app->config->{version} };
	my $request = {
		email    => $email,
		password => $password,
	};

	my $promise = Mojo::Promise->new;
	my $token;

	$ua->post_p(
		"https://traewelling.de/api/v0/auth/login" => $header => json =>
		  $request )->then(
		sub {
			my ($tx) = @_;
			if ( my $err = $tx->error ) {
				my $err_msg = "HTTP $err->{code} $err->{message}";
				$promise->reject($err_msg);
			}
			else {
				$token = $tx->result->json->{token};
				return $self->mark_trwl_login_p(
					uid   => $uid,
					email => $email,
					token => $token
				);
			}
		}
	)->then(
		sub {
			return $self->trwl_getuser_p( $uid, $token );
		}
	)->then(
		sub {
			$promise->resolve;
		}
	)->catch(
		sub {
			my ($err) = @_;
			$promise->reject($err);
		}
	)->wait;

	return $promise;
}

sub trwl_logout_p {
	my ( $self, $uid, $token ) = @_;

	my $ua = $self->ua->request_timeout(20);

	my $header = {
		'User-Agent'    => 'travelynx/' . $self->app->config->{version},
		'Authorization' => "Bearer $token",
	};
	my $request = {};

	$self->mark_trwl_logout($uid);

	my $promise = Mojo::Promise->new;

	$ua->post_p(
		"https://traewelling.de/api/v0/auth/logout" => $header => json =>
		  $request )->then(
		sub {
			my ($tx) = @_;
			if ( my $err = $tx->error ) {
				my $err_msg = "HTTP $err->{code} $err->{message}";
				$promise->reject($err_msg);
			}
			else {
				$promise->resolve;
			}
		}
	)->catch(
		sub {
			my ($err) = @_;
			$promise->reject($err);
		}
	)->wait;

	return $promise;
}

sub settings {
	my ($self) = @_;

	my $uid = $self->current_user->{id};

	if (    $self->param('action')
		and $self->validation->csrf_protect->has_error('csrf_token') )
	{
		$self->render(
			'traewelling',
			invalid => 'csrf',
		);
		return;
	}

	if ( $self->param('action') and $self->param('action') eq 'login' ) {
		my $email    = $self->param('email');
		my $password = $self->param('password');
		$self->render_later;
		$self->trwl_login_p( $uid, $email, $password )->then(
			sub {
				my $traewelling = $self->get_traewelling;
				$self->render(
					'traewelling',
					traewelling     => $traewelling,
					new_traewelling => 1,
				);
			}
		)->catch(
			sub {
				my ($err) = @_;
				$self->render(
					'traewelling',
					traewelling     => {},
					new_traewelling => 1,
					login_error     => $err,
				);
			}
		)->wait;
		return;
	}
	elsif ( $self->param('action') and $self->param('action') eq 'logout' ) {
		$self->render_later;
		my $traewelling = $self->get_traewelling;
		$self->trwl_logout_p( $uid, $traewelling->{token} )->then(
			sub {
				$self->flash( success => 'traewelling' );
				$self->redirect_to('account');
			}
		)->catch(
			sub {
				my ($err) = @_;
				$self->render(
					'traewelling',
					traewelling     => {},
					new_traewelling => 1,
					logout_error    => $err,
				);
			}
		)->wait;
		return;
	}
	elsif ( $self->param('action') and $self->param('action') eq 'config' ) {
		$self->trwl_set_sync(
			$uid,
			$self->param('push_sync') ? 1 : 0,
			$self->param('pull_sync') ? 1 : 0
		);
		$self->flash( success => 'traewelling' );
		$self->redirect_to('account');
		return;
	}

	my $traewelling = $self->get_traewelling;

	$self->param( push_sync => $traewelling->{push_sync} ? 1 : 0 );
	$self->param( pull_sync => $traewelling->{pull_sync} ? 1 : 0 );

	$self->render(
		'traewelling',
		traewelling => $traewelling,
	);
}

1;
