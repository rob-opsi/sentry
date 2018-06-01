from __future__ import absolute_import

from datetime import datetime, timedelta
from mock import patch
import pytest
import pytz
import time

from sentry.models import GroupHash, GroupHashTombstone
from sentry.testutils import SnubaTestCase
from sentry.utils import snuba


class SnubaTest(SnubaTestCase):
    def test(self):
        "This is just a simple 'hello, world' example test."

        now = datetime.now()

        events = [{
            'event_id': 'x' * 32,
            'primary_hash': '1' * 32,
            'project_id': 100,
            'message': 'message',
            'platform': 'python',
            'datetime': now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
            'data': {
                'received': time.mktime(now.timetuple()),
            }
        }]

        self.snuba_insert(events)

        assert snuba.query(
            start=now - timedelta(days=1),
            end=now + timedelta(days=1),
            groupby=['project_id'],
            filter_keys={'project_id': [100]},
        ) == {100: 1}

    def test_fail(self):
        now = datetime.now()
        with pytest.raises(snuba.SnubaError):
            snuba.query(
                start=now - timedelta(days=1),
                end=now + timedelta(days=1),
                filter_keys={'project_id': [100]},
                groupby=[")("],
            )

    @patch('django.utils.timezone.now')
    def test_get_project_issues(self, mock_time):
        now = datetime(2018, 1, 1, 0, 0, 0, tzinfo=pytz.utc)
        mock_time.return_value = now
        assert snuba.get_project_issues([self.project]) == []

        GroupHash.objects.create(
            project=self.project,
            group=self.group,
            hash='a' * 32
        )
        assert snuba.get_project_issues([self.project]) == [(1, [('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', None)])]

        GroupHashTombstone.tombstone_groups(self.project.id, [self.group.id])
        GroupHash.objects.create(
            project=self.project,
            group=self.group,
            hash='a' * 32
        )
        assert snuba.get_project_issues([self.project]) == [(1, [('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', now)])]
